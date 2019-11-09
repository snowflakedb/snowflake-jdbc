/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.core.ChunkDownloader;
import net.snowflake.client.core.DownloaderMetrics;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.core.OCSPMode;
import net.snowflake.client.core.ObjectMapperFactory;
import net.snowflake.client.core.QueryResultFormat;
import net.snowflake.client.core.SFArrowResultSet;
import net.snowflake.client.jdbc.SnowflakeResultChunk.DownloadState;
import net.snowflake.client.jdbc.telemetryOOB.TelemetryService;
import net.snowflake.client.log.ArgSupplier;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.client.util.SecretDetector;
import net.snowflake.common.core.SqlState;
import org.apache.arrow.memory.RootAllocator;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.SequenceInputStream;
import java.io.StringWriter;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

/**
 * Class for managing async download of offline result chunks
 * <p>
 * Created by jhuang on 11/12/14.
 */
public class SnowflakeChunkDownloader implements ChunkDownloader
{
  // SSE-C algorithm header
  private static final String SSE_C_ALGORITHM =
      "x-amz-server-side-encryption-customer-algorithm";

  // SSE-C customer key header
  private static final String SSE_C_KEY =
      "x-amz-server-side-encryption-customer-key";

  // SSE-C algorithm value
  private static final String SSE_C_AES = "AES256";

  // object mapper for deserialize JSON
  private static final ObjectMapper mapper =
      ObjectMapperFactory.getObjectMapper();

  /**
   * a shared JSON parser factory.
   */
  private static final JsonFactory jsonFactory = new MappingJsonFactory();

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(SnowflakeChunkDownloader.class);
  private static final int STREAM_BUFFER_SIZE = 1 * 1024 * 1024;
  private static final long SHUTDOWN_TIME = 3;
  private final SnowflakeConnectString snowflakeConnectionString;
  private final OCSPMode ocspMode;

  private JsonResultChunk.ResultChunkDataCache chunkDataCache
      = new JsonResultChunk.ResultChunkDataCache();
  private List<SnowflakeResultChunk> chunks;

  // index of next chunk to be consumed (it may not be ready yet)
  private int nextChunkToConsume = 0;

  // index of next chunk to be downloaded
  private int nextChunkToDownload = 0;

  // number of prefetch slots
  private final int prefetchSlots;

  // TRUE if JsonParserV2 should be used FALSE otherwise.
  private boolean useJsonParserV2;

  // thread pool
  private final ThreadPoolExecutor executor;

  // number of millis main thread waiting for chunks from downloader
  private long numberMillisWaitingForChunks = 0;

  // is the downloader terminated
  private boolean terminated = false;

  // number of millis spent on downloading result chunks
  private final AtomicLong totalMillisDownloadingChunks = new AtomicLong(0);

  // number of millis spent on parsing result chunks
  private final AtomicLong totalMillisParsingChunks = new AtomicLong(0);

  // The query result master key
  private final String qrmk;

  private Map<String, String> chunkHeadersMap;

  private final int networkTimeoutInMilli;

  private long memoryLimit;

  // the current memory usage across JVM
  private static Long currentMemoryUsage = 0L;

  // used to track the downloading threads
  private Map<Integer, Future> downloaderFutures = new HashMap<>();

  /**
   * query result format
   */
  private QueryResultFormat queryResultFormat;

  /**
   * Arrow memory allocator for the current resultSet
   */
  private RootAllocator rootAllocator;

  static long getCurrentMemoryUsage()
  {
    synchronized (currentMemoryUsage)
    {
      return currentMemoryUsage.longValue();
    }
  }

  // The parameters used to wait for available memory:
  // starting waiting time will be BASE_WAITING_MS * WAITING_SECS_MULTIPLIER = 100 ms
  private long BASE_WAITING_MS = 50;
  private long WAITING_SECS_MULTIPLIER = 2;
  // the maximum waiting time
  private long MAX_WAITING_MS = 30 * 1000;
  // the default jitter ratio 10%
  private long WAITING_JITTER_RATIO = 10;
  /**
   * Timeout that the main thread waits for downloading the current chunk
   */
  private static final long downloadedConditionTimeoutInSeconds = HttpUtil.getDownloadedConditionTimeoutInSeconds();

  private static final int MAX_NUM_OF_RETRY = 10;
  private static final int MAX_RETRY_JITTER = 1000; // milliseconds

  public OCSPMode getOCSPMode()
  {
    return ocspMode;
  }

  /**
   * Create a pool of downloader threads.
   *
   * @param threadNamePrefix name of threads in pool
   * @param parallel         number of thread in pool
   * @return new thread pool
   */
  private static ThreadPoolExecutor createChunkDownloaderExecutorService(
      final String threadNamePrefix, final int parallel)
  {
    ThreadFactory threadFactory = new ThreadFactory()
    {
      private int threadCount = 1;

      public Thread newThread(final Runnable r)
      {
        final Thread thread = new Thread(r);
        thread.setName(threadNamePrefix + threadCount++);

        thread.setUncaughtExceptionHandler(
            new Thread.UncaughtExceptionHandler()
            {
              public void uncaughtException(Thread t, Throwable e)
              {
                logger.error(
                    "uncaughtException in thread: " + t + " {}",
                    e);
              }
            });

        thread.setDaemon(true);

        return thread;
      }
    };
    return (ThreadPoolExecutor) Executors.newFixedThreadPool(parallel,
                                                             threadFactory);
  }

  /**
   * Constructor to initialize downloader
   *
   * @param resultSetSerializable the result set serializable object which
   *                              includes required metadata to start chunk downloader
   */
  public SnowflakeChunkDownloader(SnowflakeResultSetSerializableV1 resultSetSerializable)
  throws SnowflakeSQLException
  {
    this.snowflakeConnectionString =
        resultSetSerializable.getSnowflakeConnectString();
    this.ocspMode = resultSetSerializable.getOCSPMode();
    this.qrmk = resultSetSerializable.getQrmk();
    this.networkTimeoutInMilli = resultSetSerializable.getNetworkTimeoutInMilli();
    this.prefetchSlots = resultSetSerializable.getResultPrefetchThreads() * 2;
    this.useJsonParserV2 = resultSetSerializable.getUseJsonParserV2();
    this.memoryLimit = resultSetSerializable.getMemoryLimit();
    this.queryResultFormat = resultSetSerializable.getQueryResultFormat();
    logger.debug("qrmk = {}", this.qrmk);
    this.chunkHeadersMap = resultSetSerializable.getChunkHeadersMap();

    // create the chunks array
    this.chunks = new ArrayList<>(resultSetSerializable.getChunkFileCount());

    if (resultSetSerializable.getChunkFileCount() < 1)
    {
      throw new SnowflakeSQLException(ErrorCode.INTERNAL_ERROR,
                                      "Incorrect chunk count: " +
                                      resultSetSerializable.getChunkFileCount());
    }

    // initialize chunks with url and row count
    for (SnowflakeResultSetSerializableV1.ChunkFileMetadata chunkFileMetadata :
        resultSetSerializable.getChunkFileMetadatas())
    {
      SnowflakeResultChunk chunk;
      switch (this.queryResultFormat)
      {
        case ARROW:
          this.rootAllocator = resultSetSerializable.getRootAllocator();
          chunk = new ArrowResultChunk(chunkFileMetadata.getFileURL(),
                                       chunkFileMetadata.getRowCount(),
                                       resultSetSerializable.getColumnCount(),
                                       chunkFileMetadata.getUncompressedByteSize(),
                                       this.rootAllocator);
          break;

        case JSON:
          chunk = new JsonResultChunk(chunkFileMetadata.getFileURL(),
                                      chunkFileMetadata.getRowCount(),
                                      resultSetSerializable.getColumnCount(),
                                      chunkFileMetadata.getUncompressedByteSize(),
                                      this.useJsonParserV2);
          break;

        default:
          throw new SnowflakeSQLException(ErrorCode.INTERNAL_ERROR,
                                          "Invalid result format: " + queryResultFormat.name());
      }

      logger.debug("add chunk, url={} rowCount={} uncompressedSize={} " +
                   "neededChunkMemory={}, chunkResultFormat={}",
                   chunk.getScrubbedUrl(), chunk.getRowCount(),
                   chunk.getUncompressedSize(),
                   chunk.computeNeededChunkMemory(),
                   queryResultFormat.name());

      chunks.add(chunk);
    }
    // prefetch threads and slots from parameter settings
    int effectiveThreads = Math.min(resultSetSerializable.getResultPrefetchThreads(),
                                    resultSetSerializable.getChunkFileCount());

    logger.debug(
        "#chunks: {} #threads:{} #slots:{} -> pool:{}",
        resultSetSerializable.getChunkFileCount(),
        resultSetSerializable.getResultPrefetchThreads(),
        prefetchSlots, effectiveThreads);

    // create thread pool
    executor =
        createChunkDownloaderExecutorService("result-chunk-downloader-",
                                             effectiveThreads);

    try
    {
      startNextDownloaders();
    }
    catch (OutOfMemoryError outOfMemoryError)
    {
      logOutOfMemoryError();
      StringWriter errors = new StringWriter();
      outOfMemoryError.printStackTrace(new PrintWriter(errors));
      throw new SnowflakeSQLException(SqlState.INTERNAL_ERROR,
                                      ErrorCode.INTERNAL_ERROR.getMessageCode(),
                                      errors);
    }
  }

  /**
   * Submit download chunk tasks to executor.
   * Number depends on thread and memory limit
   */
  private void startNextDownloaders() throws SnowflakeSQLException
  {
    long waitingTime = BASE_WAITING_MS;

    // submit the chunks to be downloaded up to the prefetch slot capacity
    // and limited by memory
    while (nextChunkToDownload - nextChunkToConsume < prefetchSlots &&
           nextChunkToDownload < chunks.size())
    {
      // check if memory limit allows more prefetching
      final SnowflakeResultChunk nextChunk = chunks.get(nextChunkToDownload);
      final long neededChunkMemory = nextChunk.computeNeededChunkMemory();

      // each time only one thread can enter this block
      synchronized (currentMemoryUsage)
      {
        // make sure memoryLimit > neededChunkMemory; otherwise, the thread hangs
        if (neededChunkMemory > memoryLimit)
        {
          logger.debug("Thread {}: reset memoryLimit from {} MB to current chunk size {} MB",
                       (ArgSupplier) () -> Thread.currentThread().getId(),
                       (ArgSupplier) () -> memoryLimit / 1024 / 1024,
                       (ArgSupplier) () -> neededChunkMemory / 1024 / 1024);

          memoryLimit = neededChunkMemory;
        }

        // no memory allocate when memory is not enough for prefetch
        if (currentMemoryUsage + neededChunkMemory > memoryLimit &&
            nextChunkToDownload - nextChunkToConsume > 0)
        {
          break;
        }

        // only allocate memory when the future usage is less than the limit
        if (currentMemoryUsage + neededChunkMemory <= memoryLimit)
        {
          if (queryResultFormat == QueryResultFormat.JSON)
          {
            ((JsonResultChunk) nextChunk).tryReuse(chunkDataCache);
          }

          currentMemoryUsage += neededChunkMemory;

          logger.debug("Thread {}: currentMemoryUsage in MB: {}, nextChunkToDownload: {}, " +
                       "nextChunkToConsume: {}, newReservedMemory in B: {} ",
                       (ArgSupplier) () -> Thread.currentThread().getId(),
                       (ArgSupplier) () -> currentMemoryUsage / 1024 / 1024,
                       nextChunkToDownload,
                       nextChunkToConsume,
                       neededChunkMemory);

          logger.debug("submit chunk #{} for downloading, url={}",
                       this.nextChunkToDownload, nextChunk.getScrubbedUrl());

          Future downloaderFuture = executor.submit(getDownloadChunkCallable(this,
                                                                             nextChunk,
                                                                             qrmk, nextChunkToDownload,
                                                                             chunkHeadersMap,
                                                                             networkTimeoutInMilli));
          downloaderFutures.put(nextChunkToDownload, downloaderFuture);
          // increment next chunk to download
          nextChunkToDownload++;
          // make sure reset waiting time
          waitingTime = BASE_WAITING_MS;
          // go to next chunk
          continue;
        }
      }

      // waiting when nextChunkToDownload is equal to nextChunkToConsume but reach memory limit
      try
      {
        waitingTime *= WAITING_SECS_MULTIPLIER;
        waitingTime = waitingTime > MAX_WAITING_MS ? MAX_WAITING_MS : waitingTime;
        long jitter = ThreadLocalRandom.current().nextLong(0, waitingTime / WAITING_JITTER_RATIO);
        waitingTime += jitter;
        if (logger.isDebugEnabled())
        {
          logger.debug("Thread {} waiting for {}s: currentMemoryUsage in MB: {}, neededChunkMemory in MB: {}, " +
                       "nextChunkToDownload: {}, nextChunkToConsume: {} ",
                       Thread.currentThread().getId(),
                       waitingTime / 1000.0,
                       currentMemoryUsage / 1024 / 1024,
                       neededChunkMemory / 1024 / 1024,
                       nextChunkToDownload,
                       nextChunkToConsume);
        }
        Thread.sleep(waitingTime);
      }
      catch (InterruptedException ie)
      {
        throw new SnowflakeSQLException(
            SqlState.INTERNAL_ERROR,
            ErrorCode.INTERNAL_ERROR.getMessageCode(),
            "Waiting SnowflakeChunkDownloader has been interrupted.");
      }
    }

    // clear the cache, we can't download more at the moment
    // so we won't need them in the near future
    chunkDataCache.clear();
  }

  private void releaseCurrentMemoryUsage(int chunkId, long releaseSize)
  {
    if (releaseSize > 0)
    {
      synchronized (currentMemoryUsage)
      {
        // has to be before reusing the memory
        currentMemoryUsage -= releaseSize;
        logger.debug("Thread {}: currentMemoryUsage in MB: {}, released in MB: {}, chunk: {}",
                     (ArgSupplier) () -> Thread.currentThread().getId(),
                     (ArgSupplier) () -> currentMemoryUsage / 1024 / 1024,
                     releaseSize,
                     chunkId);
      }
    }
  }

  /**
   * release all existing chunk memory usage before close
   */
  private void releaseAllChunkMemoryUsage()
  {
    if (chunks == null || chunks.size() == 0)
    {
      return;
    }

    // only release the chunks has been downloading or downloaded
    for (int i = 0; i < nextChunkToDownload; i++)
    {
      releaseCurrentMemoryUsage(i, chunks.get(i).computeNeededChunkMemory());
    }
  }

  /**
   * The method does the following:
   * <p>
   * 1. free the previous chunk data and submit a new chunk to be downloaded
   * <p>
   * 2. get next chunk to consume, if it is not ready for consumption,
   * it waits until it is ready
   *
   * @return next SnowflakeResultChunk to be consumed
   * @throws InterruptedException  if downloading thread was interrupted
   * @throws SnowflakeSQLException if downloader encountered an error
   */
  public SnowflakeResultChunk getNextChunkToConsume() throws InterruptedException,
                                                             SnowflakeSQLException
  {
    // free previous chunk data and submit a new chunk for downloading
    if (this.nextChunkToConsume > 0)
    {
      int prevChunk = this.nextChunkToConsume - 1;

      // free the chunk data for previous chunk
      logger.debug("free chunk data for chunk #{}",
                   prevChunk);

      long chunkMemUsage = chunks.get(prevChunk).computeNeededChunkMemory();

      // reuse chunkcache if json result
      if (this.queryResultFormat == QueryResultFormat.JSON)
      {
        if (this.nextChunkToDownload < this.chunks.size())
        {
          // Reuse the set of object to avoid reallocation
          // It is important to do this BEFORE starting the next download
          chunkDataCache.add((JsonResultChunk) this.chunks.get(prevChunk));
        }
        else
        {
          // clear the cache if we don't need it anymore
          chunkDataCache.clear();
        }
      }

      // Free any memory the previous chunk might hang on
      this.chunks.get(prevChunk).freeData();

      releaseCurrentMemoryUsage(prevChunk, chunkMemUsage);

    }

    // if no more chunks, return null
    if (this.nextChunkToConsume >= this.chunks.size())
    {
      logger.debug("no more chunk");
      return null;
    }

    // prefetch next chunks
    try
    {
      startNextDownloaders();
    }
    catch (OutOfMemoryError outOfMemoryError)
    {
      logOutOfMemoryError();
      StringWriter errors = new StringWriter();
      outOfMemoryError.printStackTrace(new PrintWriter(errors));
      throw new SnowflakeSQLException(SqlState.INTERNAL_ERROR,
                                      ErrorCode.INTERNAL_ERROR.getMessageCode(),
                                      errors);
    }

    SnowflakeResultChunk currentChunk = this.chunks.get(nextChunkToConsume);

    if (currentChunk.getDownloadState() == DownloadState.SUCCESS)
    {
      logger.debug("chunk #{} is ready to consume", nextChunkToConsume);
      nextChunkToConsume++;
      if (nextChunkToConsume == this.chunks.size())
      {
        // make sure to release the last chunk
        releaseCurrentMemoryUsage(
            nextChunkToConsume - 1, chunks.get(nextChunkToConsume - 1).computeNeededChunkMemory());
      }
      return currentChunk;
    }
    else
    {
      // the chunk we want to consume is not ready yet, wait for it
      currentChunk.getLock().lock();
      try
      {
        logger.debug("#chunk{} is not ready to consume",
                     nextChunkToConsume);
        logger.debug("consumer get lock to check chunk state");

        waitForChunkReady(currentChunk);

        // downloader thread encountered an error
        if (currentChunk.getDownloadState() == DownloadState.FAILURE)
        {
          logger.error("downloader encountered error: {}",
                       currentChunk.getDownloadError());

          if (currentChunk.getDownloadError().contains("java.lang.OutOfMemoryError: Java heap space"))
          {
            logOutOfMemoryError();
          }

          throw new SnowflakeSQLException(SqlState.INTERNAL_ERROR,
                                          ErrorCode.INTERNAL_ERROR.getMessageCode(),
                                          currentChunk.getDownloadError());
        }

        logger.debug("#chunk{} is ready to consume",
                     nextChunkToConsume);

        nextChunkToConsume++;

        // next chunk to consume is ready for consumption
        return currentChunk;
      }
      finally
      {
        logger.debug("consumer free lock");

        boolean terminateDownloader = (currentChunk.getDownloadState() == DownloadState.FAILURE);
        // release the unlock always
        currentChunk.getLock().unlock();
        if (nextChunkToConsume == this.chunks.size())
        {
          // make sure to release the last chunk
          releaseCurrentMemoryUsage(
              nextChunkToConsume - 1, chunks.get(nextChunkToConsume - 1).computeNeededChunkMemory());
        }
        if (terminateDownloader)
        {
          logger.debug("Download result fail. Shut down the chunk downloader");
          terminate();
        }
      }
    }
  }

  /**
   * wait for the current chunk to be ready to consume
   * if the downloader fails then let it retry for at most 10 times
   * if the downloader is in progress for at most one hour or the downloader has already retried more than 10 times,
   * then throw an exception.
   *
   * @param currentChunk
   * @throws InterruptedException
   */
  private void waitForChunkReady(SnowflakeResultChunk currentChunk) throws InterruptedException
  {
    int retry = 0;
    long startTime = System.currentTimeMillis();
    while (currentChunk.getDownloadState() != DownloadState.SUCCESS &&
           retry < MAX_NUM_OF_RETRY)
    {
      logger.debug("Thread {} is waiting for #chunk{} to be ready, current"
                   + "chunk state is: {}, retry={}",
                   Thread.currentThread().getId(),
                   nextChunkToConsume, currentChunk.getDownloadState(), retry);

      if (currentChunk.getDownloadState() != DownloadState.FAILURE)
      {
        // if the state is not failure, we should keep waiting; otherwise, we skip waiting
        if (!currentChunk.getDownloadCondition().await(downloadedConditionTimeoutInSeconds, TimeUnit.SECONDS))
        {
          // if the current chunk has not condition change over the timeout (which is rare)
          logger.debug("Thread {} is timeout for waiting #chunk{} to be ready, current"
                       + "chunk state is: {}, retry={}",
                       Thread.currentThread().getId(),
                       nextChunkToConsume, currentChunk.getDownloadState(), retry);

          currentChunk.setDownloadState(DownloadState.FAILURE);
          currentChunk.setDownloadError(String.format("Timeout waiting for the download of #chunk%d" +
                                                      "(Total chunks: %d) retry=%d", nextChunkToConsume,
                                                      this.chunks.size(), retry));
          break;
        }
      }

      if (currentChunk.getDownloadState() != DownloadState.SUCCESS)
      {
        // timeout or failed
        retry++;
        logger.debug("Since downloadState is {} Thread {} decides to retry {} time(s) for #chunk{}",
                     currentChunk.getDownloadState(),
                     Thread.currentThread().getId(),
                     retry,
                     nextChunkToConsume);
        Future downloaderFuture = downloaderFutures.get(nextChunkToConsume);
        if (downloaderFuture != null)
        {
          downloaderFuture.cancel(true);
        }
        HttpUtil.closeExpiredAndIdleConnections();

        chunks.get(nextChunkToConsume).getLock().lock();
        try
        {
          chunks.get(nextChunkToConsume).setDownloadState(DownloadState.IN_PROGRESS);
          chunks.get(nextChunkToConsume).reset();
        }
        finally
        {
          chunks.get(nextChunkToConsume).getLock().unlock();
        }

        // random jitter before start next retry
        Thread.sleep(new Random().nextInt(MAX_RETRY_JITTER));

        downloaderFuture = executor.submit(getDownloadChunkCallable(this,
                                                                    chunks.get(nextChunkToConsume),
                                                                    qrmk, nextChunkToConsume,
                                                                    chunkHeadersMap,
                                                                    networkTimeoutInMilli));
        downloaderFutures.put(nextChunkToDownload, downloaderFuture);
      }
    }
    if (currentChunk.getDownloadState() == DownloadState.SUCCESS)
    {
      logger.debug("ready to consume #chunk{}, succeed retry={}", nextChunkToConsume, retry);
    }
    else if (retry >= MAX_NUM_OF_RETRY)
    {
      // stop retrying and report failure
      currentChunk.setDownloadState(DownloadState.FAILURE);
      currentChunk.setDownloadError(
          String.format("Max retry reached for the download of #chunk%d " +
                        "(Total chunks: %d) retry=%d, error=%s",
                        nextChunkToConsume, this.chunks.size(), retry,
                        chunks.get(nextChunkToConsume).getDownloadError()));
    }
    this.numberMillisWaitingForChunks +=
        (System.currentTimeMillis() - startTime);
  }

  /**
   * log out of memory error and provide the suggestion to avoid this error
   */
  private void logOutOfMemoryError()
  {
    logger.error("Dump some crucial information below:\n" +
                 "Total milliseconds waiting for chunks: {},\n" +
                 "Total memory used: {}, Max heap size: {}, total download time: {} millisec,\n" +
                 "total parsing time: {} milliseconds, total chunks: {},\n" +
                 "currentMemoryUsage in Byte: {}, currentMemoryLimit in Bytes: {} \n" +
                 "nextChunkToDownload: {}, nextChunkToConsume: {}\n" +
                 "Several suggestions to try to resolve the OOM issue:\n" +
                 "1. increase the JVM heap size if you have more space; or \n" +
                 "2. use CLIENT_MEMORY_LIMIT to reduce the memory usage by the JDBC driver " +
                 "(https://docs.snowflake.net/manuals/sql-reference/parameters.html#client-memory-limit)" +
                 "3. please make sure 2 * CLIENT_PREFETCH_THREADS * CLIENT_RESULT_CHUNK_SIZE < CLIENT_MEMORY_LIMIT. " +
                 "If not, please reduce CLIENT_PREFETCH_THREADS and CLIENT_RESULT_CHUNK_SIZE too.",
                 numberMillisWaitingForChunks,
                 Runtime.getRuntime().totalMemory(), Runtime.getRuntime().maxMemory(),
                 totalMillisDownloadingChunks.get(),
                 totalMillisParsingChunks.get(), chunks.size(), currentMemoryUsage, memoryLimit,
                 nextChunkToDownload, nextChunkToConsume);
  }

  /**
   * terminate the downloader
   *
   * @return chunk downloader metrics collected over instance lifetime
   */
  @Override
  public DownloaderMetrics terminate() throws InterruptedException
  {
    if (!terminated)
    {
      releaseAllChunkMemoryUsage();

      logger.debug("Total milliseconds waiting for chunks: {}, " +
                   "Total memory used: {}, total download time: {} millisec, " +
                   "total parsing time: {} milliseconds, total chunks: {}",
                   numberMillisWaitingForChunks,
                   Runtime.getRuntime().totalMemory(), totalMillisDownloadingChunks.get(),
                   totalMillisParsingChunks.get(), chunks.size());

      if (executor != null)
      {
        if (!executor.isShutdown())
        {
          // cancel running downloaders
          downloaderFutures.forEach((k, v) -> v.cancel(true));
          // shutdown executor
          executor.shutdown();
          if (!executor.awaitTermination(SHUTDOWN_TIME, TimeUnit.SECONDS))
          {
            logger.debug("Executor did not terminate in the specified time.");

            List<Runnable> droppedTasks = executor.shutdownNow(); //optional **
            logger.debug(
                "Executor was abruptly shut down. " + droppedTasks.size() +
                    " tasks will not be executed."); //optional **
          }
        }
      }
      for (SnowflakeResultChunk chunk : chunks)
      {
        // explicitly free each chunk since Arrow chunk may hold direct memory
        chunk.freeData();
      }

      if (queryResultFormat == QueryResultFormat.ARROW)
      {
        SFArrowResultSet.closeRootAllocator(rootAllocator);
      }
      else
      {
        chunkDataCache.clear();
      }

      chunks = null;

      terminated = true;
      return new DownloaderMetrics(numberMillisWaitingForChunks,
                                   totalMillisDownloadingChunks.get(),
                                   totalMillisParsingChunks.get());
    }
    return null;
  }

  /**
   * add download time
   *
   * @param downloadTime Time for downloading a single chunk
   */
  private void addDownloadTime(long downloadTime)
  {
    this.totalMillisDownloadingChunks.addAndGet(downloadTime);
  }

  /**
   * add parsing time
   *
   * @param parsingTime Time for parsing a single chunk
   */
  private void addParsingTime(long parsingTime)
  {
    this.totalMillisParsingChunks.addAndGet(parsingTime);
  }

  /**
   * Create a download callable that will be run in download thread
   *
   * @param downloader            object to download the chunk
   * @param resultChunk           object contains information about the chunk will
   *                              be downloaded
   * @param qrmk                  Query Result Master Key
   * @param chunkIndex            the index of the chunk which will be downloaded in array
   *                              chunks. This is mainly for logging purpose
   * @param chunkHeadersMap       contains headers needed to be added when downloading from s3
   * @param networkTimeoutInMilli network timeout
   * @return A callable responsible for downloading chunk
   */
  private static Callable<Void> getDownloadChunkCallable(
      final SnowflakeChunkDownloader downloader,
      final SnowflakeResultChunk resultChunk,
      final String qrmk, final int chunkIndex,
      final Map<String, String> chunkHeadersMap,
      final int networkTimeoutInMilli)
  {
    return new Callable<Void>()
    {
      /**
       *  Step 1. use chunk url to get the input stream
       * @return
       * @throws SnowflakeSQLException
       */
      private InputStream getInputStream() throws SnowflakeSQLException
      {
        HttpResponse response;
        try
        {
          response = getResultChunk(resultChunk.getUrl());
        }
        catch (URISyntaxException | IOException ex)
        {
          throw new SnowflakeSQLException(SqlState.IO_ERROR,
                                          ErrorCode.NETWORK_ERROR
                                              .getMessageCode(),
                                          "Error encountered when request a result chunk URL: "
                                          + resultChunk.getUrl()
                                          + " " + ex.getLocalizedMessage());
        }

        /*
         * return error if we don't get a response or the response code
         * means failure.
         */
        if (response == null
            || response.getStatusLine().getStatusCode() != 200)
        {
          logger.error("Error fetching chunk from: {}",
                       resultChunk.getScrubbedUrl());

          SnowflakeUtil.logResponseDetails(response, logger);

          throw new SnowflakeSQLException(SqlState.IO_ERROR,
                                          ErrorCode.NETWORK_ERROR
                                              .getMessageCode(),
                                          "Error encountered when downloading a result chunk: HTTP "
                                          + "status="
                                          + ((response != null)
                                             ? response.getStatusLine().getStatusCode()
                                             : "null response"));
        }

        InputStream inputStream;
        final HttpEntity entity = response.getEntity();
        try
        {
          // read the chunk data
          InputStream is = entity.getContent();

          // Determine the format of the response, if it is not
          // either plain text or gzip, raise an error.
          Header encoding = response.getFirstHeader("Content-Encoding");
          if (encoding != null)
          {
            if (encoding.getValue().equalsIgnoreCase("gzip"))
            {
              /* specify buffer size for GZIPInputStream */
              is = new GZIPInputStream(is, STREAM_BUFFER_SIZE);
            }
            else
            {
              throw
                  new SnowflakeSQLException(
                      SqlState.INTERNAL_ERROR,
                      ErrorCode.INTERNAL_ERROR.getMessageCode(),
                      "Exception: unexpected compression got " +
                      encoding.getValue());
            }
          }

          if (downloader.useJsonParserV2 ||
              downloader.queryResultFormat == QueryResultFormat.ARROW)
          {
            inputStream = is;
          }
          else
          {
            // Build a sequence of streams to wrap the input stream
            // with '[' ... ']' to be able to plug this in the
            // Jackson JSON parser.
            // gzip stream uses 64KB
            // no buffering as json parser does it internally
            inputStream =
                new SequenceInputStream(
                    Collections.enumeration(Arrays.asList(
                        new ByteArrayInputStream("[".getBytes(
                            StandardCharsets.UTF_8)),
                        is,
                        new ByteArrayInputStream("]".getBytes(
                            StandardCharsets.UTF_8)))));
          }
        }
        catch (Exception ex)
        {
          logger.error(
              "Failed to decompress data: {}",
              response);

          throw
              new SnowflakeSQLException(
                  SqlState.INTERNAL_ERROR,
                  ErrorCode.INTERNAL_ERROR.getMessageCode(),
                  "Failed to decompress data: " +
                  response.toString());
        }

        // trace the response if requested
        logger.debug("Json response: {}", response);

        return inputStream;
      }

      /**
       * Read the input stream and parse chunk data into memory
       * @param inputStream
       * @throws SnowflakeSQLException
       */
      private void downloadAndParseChunk(InputStream inputStream) throws SnowflakeSQLException
      {
        // remember the download time
        resultChunk.setDownloadTime(System.currentTimeMillis() - startTime);
        downloader.addDownloadTime(resultChunk.getDownloadTime());

        startTime = System.currentTimeMillis();

        // parse the result json
        try
        {
          if (downloader.queryResultFormat == QueryResultFormat.ARROW)
          {
            ((ArrowResultChunk) resultChunk).readArrowStream(inputStream);
          }
          else
          {
            if (downloader.useJsonParserV2)
            {
              parseJsonToChunkV2(inputStream, resultChunk);
            }
            else
            {
              parseJsonToChunk(inputStream, resultChunk);
            }
          }
        }
        catch (Exception ex)
        {
          logger.debug("Thread {} Exception when parsing result #chunk{}: {}",
                       Thread.currentThread().getId(), chunkIndex, ex.getLocalizedMessage());

          throw new SnowflakeSQLException(ex, SqlState.INTERNAL_ERROR,
                                          ErrorCode.INTERNAL_ERROR
                                              .getMessageCode(),
                                          "Exception: " +
                                          ex.getLocalizedMessage());
        }
        finally
        {
          // close the buffer reader will close underlying stream
          logger.debug("Thread {} close input stream for #chunk{}",
                       Thread.currentThread().getId(),
                       chunkIndex);
          try
          {
            inputStream.close();
          }
          catch (IOException ex)
          {
            throw new SnowflakeSQLException(ex, SqlState.INTERNAL_ERROR,
                                            ErrorCode.INTERNAL_ERROR
                                                .getMessageCode(),
                                            "Exception: " +
                                            ex.getLocalizedMessage());
          }
        }

        // add parsing time
        resultChunk.setParseTime(System.currentTimeMillis() - startTime);
        downloader.addParsingTime(resultChunk.getParseTime());
      }

      private long startTime;

      public Void call()
      {
        resultChunk.getLock().lock();
        try
        {
          resultChunk.setDownloadState(DownloadState.IN_PROGRESS);
        }
        finally
        {
          resultChunk.getLock().unlock();
        }

        logger.debug("Downloading #chunk{}, url={}, Thread {}",
                     chunkIndex, resultChunk.getUrl(), Thread.currentThread().getId());

        startTime = System.currentTimeMillis();

        // initialize the telemetry service for this downloader thread using the main telemetry service
        TelemetryService.getInstance().updateContext(downloader.snowflakeConnectionString);

        try
        {
          InputStream is = getInputStream();
          logger.debug("Thread {} start downloading #chunk{}",
                       Thread.currentThread().getId(),
                       chunkIndex);
          downloadAndParseChunk(is);
          logger.debug("Thread {} finish downloading #chunk{}",
                       Thread.currentThread().getId(),
                       chunkIndex);
          downloader.downloaderFutures.remove(chunkIndex);
          logger.debug(
              "Finished preparing chunk data for {}, " +
              "total download time={}ms, total parse time={}ms",
              resultChunk.getScrubbedUrl(),
              resultChunk.getDownloadTime(),
              resultChunk.getParseTime());

          resultChunk.getLock().lock();
          try
          {
            logger.debug(
                "get lock to change the chunk to be ready to consume");

            logger.debug(
                "wake up consumer if it is waiting for a chunk to be "
                + "ready");

            resultChunk.setDownloadState(DownloadState.SUCCESS);
            resultChunk.getDownloadCondition().signal();
          }
          finally
          {
            logger.debug(
                "Downloaded #chunk{}, free lock", chunkIndex);

            resultChunk.getLock().unlock();
          }
        }
        catch (SnowflakeSQLException ex)
        {
          resultChunk.getLock().lock();
          try
          {
            logger.debug("get lock to set chunk download error");
            resultChunk.setDownloadState(DownloadState.FAILURE);
            StringWriter errors = new StringWriter();
            ex.printStackTrace(new PrintWriter(errors));
            resultChunk.setDownloadError(errors.toString());

            logger.debug(
                "wake up consumer if it is waiting for a chunk to be ready");

            resultChunk.getDownloadCondition().signal();
          }
          finally
          {
            logger.debug("Failed to download #chunk{}, free lock",
                         chunkIndex);
            resultChunk.getLock().unlock();
          }

          logger.debug(
              "Thread {} Exception encountered ({}:{}) fetching #chunk{} from: {}, Error {}",
              Thread.currentThread().getId(),
              ex.getClass().getName(),
              ex.getLocalizedMessage(),
              chunkIndex,
              resultChunk.getScrubbedUrl(),
              resultChunk.getDownloadError());
        }

        return null;
      }

      private void parseJsonToChunkV2(InputStream jsonInputStream,
                                      SnowflakeResultChunk resultChunk)
      throws IOException, SnowflakeSQLException
      {
        /*
         * This is a hand-written binary parser that
         * handle.
         *   [ "c1", "c2", null, ... ],
         *   [ null, "c2", "c3", ... ],
         *   ...
         *   [ "c1", "c2", "c3", ... ],
         * in UTF-8
         * The number of rows is known and the number of expected columns
         * is also known.
         */
        ResultJsonParserV2 jp = new ResultJsonParserV2();
        jp.startParsing((JsonResultChunk) resultChunk);

        byte[] buf = new byte[STREAM_BUFFER_SIZE];
        int len;
        logger.debug("Thread {} start to read inputstream for #chunk{}",
                     Thread.currentThread().getId(), chunkIndex);
        while ((len = jsonInputStream.read(buf)) != -1)
        {
          jp.continueParsing(ByteBuffer.wrap(buf, 0, len));
        }
        logger.debug("Thread {} finish reading inputstream for #chunk{}",
                     Thread.currentThread().getId(), chunkIndex);
        jp.endParsing();
      }

      private void parseJsonToChunk(InputStream jsonInputStream,
                                    SnowflakeResultChunk resultChunk)
      throws IOException, SnowflakeSQLException
      {
        /*
         * This is a hand-written customized parser that
         * handle.
         * [
         *   [ "c1", "c2", null, ... ],
         *   [ null, "c2", "c3", ... ],
         *   ...
         *   [ "c1", "c2", "c3", ... ],
         * ]
         * The number of rows is known and the number of expected columns
         * is also known.
         */
        try (JsonParser jp = jsonFactory.createParser(new InputStreamReader(jsonInputStream, "UTF-8")))
        {
          JsonToken currentToken;

          // Get the first token and make sure it is the start of an array
          currentToken = jp.nextToken();
          if (currentToken != JsonToken.START_ARRAY)
          {
            throw
                new SnowflakeSQLException(
                    SqlState.INTERNAL_ERROR,
                    ErrorCode.INTERNAL_ERROR.getMessageCode(),
                    "Exception1: expected '[' " +
                    "got " +
                    currentToken.asString());
          }

          // For all the rows...
          while (jp.nextToken() != JsonToken.END_ARRAY)
          {
            // Position to the current row in the result
            ((JsonResultChunk) resultChunk).addRow(
                mapper.readValue(jp, Object[].class));
          }
          ((JsonResultChunk) resultChunk).ensureRowsComplete();
        }
      }

      private HttpResponse getResultChunk(String chunkUrl)
      throws URISyntaxException, IOException, SnowflakeSQLException
      {
        URIBuilder uriBuilder = new URIBuilder(chunkUrl);

        HttpGet httpRequest = new HttpGet(uriBuilder.build());

        if (chunkHeadersMap != null && chunkHeadersMap.size() != 0)
        {
          for (Map.Entry<String, String> entry : chunkHeadersMap.entrySet())
          {
            logger.debug("Adding header key={}, value={}",
                         entry.getKey(), entry.getValue());
            httpRequest.addHeader(entry.getKey(), entry.getValue());
          }
        }
        // Add SSE-C headers
        else if (qrmk != null)
        {
          httpRequest.addHeader(SSE_C_ALGORITHM, SSE_C_AES);
          httpRequest.addHeader(SSE_C_KEY, qrmk);
          logger.debug("Adding SSE-C headers");
        }

        logger.debug("Thread {} Fetching result #chunk{}: {}",
                     Thread.currentThread().getId(),
                     chunkIndex,
                     resultChunk.getScrubbedUrl());

        //TODO move this s3 request to HttpUtil class. In theory, upper layer
        //TODO does not need to know about http client
        CloseableHttpClient httpClient = HttpUtil.getHttpClient(downloader.getOCSPMode());

        // fetch the result chunk
        HttpResponse response =
            RestRequest.execute(httpClient,
                                httpRequest,
                                networkTimeoutInMilli / 1000, // retry timeout
                                0, // no socketime injection
                                null, // no canceling
                                false, // no cookie
                                false, // no retry
                                false, // no request_guid
                                true // retry on HTTP403 for AWS S3
            );

        logger.debug("Thread {} Call #chunk{} returned for URL: {}, response={}",
                     Thread.currentThread().getId(),
                     chunkIndex,
                     (ArgSupplier) () -> SecretDetector.maskSASToken(chunkUrl),
                     response);
        return response;
      }
    };
  }

  /**
   * This is a No Operation chunk downloader to avoid potential null pointer exception
   */
  public static class NoOpChunkDownloader implements ChunkDownloader
  {
    @Override
    public SnowflakeResultChunk getNextChunkToConsume()
    throws SnowflakeSQLException
    {
      return null;
    }

    @Override
    public DownloaderMetrics terminate()
    {
      return null;
    }
  }
}
