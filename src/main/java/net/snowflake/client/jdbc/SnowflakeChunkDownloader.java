/*
 * Copyright (c) 2012-2018 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.core.HttpUtil;
import net.snowflake.client.jdbc.SnowflakeResultChunk.DownloadState;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.SqlState;
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
import java.io.SequenceInputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

/**
 * Class for managing async download of offline result chunks
 *
 * Created by jhuang on 11/12/14.
 */
public class SnowflakeChunkDownloader
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
  private static final ObjectMapper mapper = new ObjectMapper();

  /** a shared JSON parser factory. */
  private static final JsonFactory jsonFactory  = new MappingJsonFactory();

  private static final SFLogger logger =
      SFLoggerFactory.getLogger(SnowflakeChunkDownloader.class);

  private SnowflakeResultChunk.ResultChunkDataCache chunkDataCache
      = new SnowflakeResultChunk.ResultChunkDataCache();
  private List<SnowflakeResultChunk> chunks = null;

  // index of next chunk to be consumed (it may not be ready yet)
  private int nextChunkToConsume = 0;

  // index of next chunk to be downloaded
  private int nextChunkToDownload = 0;

  // number of prefetch slots
  private final int prefetchSlots;

  // TRUE if JsonParser should be used FALSE otherwise.
  private boolean useJsonParser = false;

  // thread pool
  private ThreadPoolExecutor executor;

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

  private Map<String, String> chunkHeadersMap = null;

  private final int networkTimeoutInMilli;

  private long memoryLimit;
  private long currentMemoryUsage = 0;

  /** Timeout that main thread wait for downloading */
  private final long downloadedConditionTimeoutInSeconds = 3600;

  /**
   * Create a pool of downloader threads.
   *
   * @param threadNamePrefix name of threads in pool
   * @param parallel number of thread in pool
   * @return new thread pool
   */
  private static ThreadPoolExecutor createChunkDownloaderExecutorService(
      final String threadNamePrefix, final int parallel)
  {
    ThreadFactory threadFactory = new ThreadFactory() {
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

  public class Metrics
  {
    public final long millisWaiting;
    public final long millisDownloading;
    public final long millisParsing;
    private Metrics()
    {
      SnowflakeChunkDownloader outer = SnowflakeChunkDownloader.this;
      millisWaiting = outer.numberMillisWaitingForChunks;
      millisDownloading = outer.totalMillisDownloadingChunks.get();
      millisParsing = outer.totalMillisParsingChunks.get();
    }
  }

  /**
   * Constructor to initialize downloader
   * @param colCount number of columns to expect
   * @param chunksData JSON object contains all the chunk information
   * @param prefetchThreads number of prefetch threads
   * @param qrmk Query Result Master Key
   * @param chunkHeaders JSON object contains information about chunk headers
   * @param networkTimeoutInMilli network timeout
   * @param useJsonParser should JsonParser be used instead of object
   * @param memoryLimit memory limit for chunk buffer
   * @param efficientChunkStorage use new efficient storage format
   */
  public SnowflakeChunkDownloader(int colCount,
                                  JsonNode chunksData,
                                  int prefetchThreads,
                                  String qrmk,
                                  JsonNode chunkHeaders,
                                  int networkTimeoutInMilli,
                                  boolean useJsonParser,
                                  long memoryLimit,
                                  boolean efficientChunkStorage)
  {
    this.qrmk = qrmk;
    this.networkTimeoutInMilli = networkTimeoutInMilli;
    this.prefetchSlots = prefetchThreads * 2;
    this.useJsonParser = useJsonParser;
    this.memoryLimit = Math.min(memoryLimit, (long)(Runtime.getRuntime().maxMemory() * .8));

    logger.debug( "qrmk = {}", qrmk);

    if (chunkHeaders != null && !chunkHeaders.isMissingNode())
    {
      chunkHeadersMap = new HashMap<>(2);

      Iterator<Map.Entry<String, JsonNode>> chunkHeadersIter =
      chunkHeaders.fields();

      while (chunkHeadersIter.hasNext())
      {
        Map.Entry<String, JsonNode> chunkHeader = chunkHeadersIter.next();

        logger.debug("add header key={}, value={}",
                               new Object[]{chunkHeader.getKey(),
                               chunkHeader.getValue().asText()});
        chunkHeadersMap.put(chunkHeader.getKey(),
                            chunkHeader.getValue().asText());
      }
    }

    // no chunk data
    if (chunksData == null)
    {
      logger.debug("no chunk data");
      return;
    }

    // number of chunks
    int numChunks = chunksData.size();
    // create the chunks array
    chunks = new ArrayList<>(numChunks);

    // initialize chunks with url and row count
    for (int idx = 0; idx < numChunks; idx++)
    {
      JsonNode chunkNode = chunksData.get(idx);

      SnowflakeResultChunk chunk =
          new SnowflakeResultChunk(
              chunkNode.path("url").asText(),
              chunkNode.path("rowCount").asInt(),
              colCount,
              chunkNode.path("uncompressedSize").asInt(),
              efficientChunkStorage);

      logger.debug("add chunk, url={} rowCount={}",
          new Object[]{chunk.getUrl(), chunk.getRowCount()});

      chunks.add(chunk);
    }
    // prefetch threads and slots from parameter settings
    int effectiveThreads = Math.min(prefetchThreads, numChunks);

    logger.debug(
	       "#chunks: {} #threads:{} #slots:{} -> pool:{}", 
		   numChunks, prefetchThreads, prefetchSlots, effectiveThreads);

    // create thread pool
    executor =
        createChunkDownloaderExecutorService("result-chunk-downloader-",
                                             effectiveThreads);

    startNextDownloaders();
  }

  /**
   * Submit download chunk tasks to executor.
   * Number depends on thread and memory limit
   */
  private void startNextDownloaders()
  {
    // start downloading chunks up to number of slots
    logger.debug("Submit {} chunks to be pre-fetched",
               Math.min(prefetchSlots, chunks.size()));

    // submit the chunks to be downloaded up to the prefetch slot capacity
    // and limited by memory
    while (nextChunkToDownload - nextChunkToConsume < prefetchSlots &&
        nextChunkToDownload < chunks.size())
    {
      // check if memory limit allows more prefetching
      final SnowflakeResultChunk nextChunk = chunks.get(nextChunkToDownload);
      final long neededChunkMemory = nextChunk.computeNeededChunkMemory();
      if (currentMemoryUsage + neededChunkMemory > memoryLimit &&
          nextChunkToDownload - nextChunkToConsume > 0)
      {
        break;
      }
      nextChunk.tryReuse(chunkDataCache);

      currentMemoryUsage += neededChunkMemory;

      logger.debug("submit chunk #{} for downloading, url={}",
                 this.nextChunkToDownload, nextChunk.getUrl());

      executor.submit(getDownloadChunkCallable(this,
                                               nextChunk,
                                               qrmk, nextChunkToDownload,
                                               chunkHeadersMap,
                                               networkTimeoutInMilli));

      // increment next chunk to download
      nextChunkToDownload++;
    }

    // clear the cache, we can't download more at the moment
    // so we won't need them in the near future
    chunkDataCache.clear();
  }

  /**
   *
   * The method does the following:
   *
   * 1. free the previous chunk data and submit a new chunk to be downloaded
   *
   * 2. get next chunk to consume, if it is not ready for consumption,
   * it waits until it is ready
   *
   * @return next SnowflakeResultChunk to be consumed
   * @throws InterruptedException if downloading thread was interrupted
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

      // has to be before reusing the memory
      currentMemoryUsage -= chunks.get(prevChunk).computeNeededChunkMemory();

      if (this.nextChunkToDownload < this.chunks.size())
      {
        // Reuse the set of object to avoid reallocation
        // It is important to do this BEFORE starting the next download
        chunkDataCache.add(this.chunks.get(prevChunk));
      }
      else
      {
        // clear the cache if we don't need it anymore
        chunkDataCache.clear();
      }

      // Free any memory the previous chunk might hang on
      this.chunks.get(prevChunk).freeData();
    }

    // if no more chunks, return null
    if (this.nextChunkToConsume >= this.chunks.size())
    {
      logger.debug("no more chunk");
      return null;
    }

    // prefetch next chunks
    startNextDownloaders();

    SnowflakeResultChunk currentChunk = this.chunks.get(nextChunkToConsume);

    if (currentChunk.getDownloadState() == DownloadState.SUCCESS)
    {
      logger.debug("chunk #{} is ready to consume", nextChunkToConsume);
      nextChunkToConsume++;
      return currentChunk;
    }
    else
    {
      // the chunk we want to consume is not ready yet, wait for it
      try
      {
        logger.debug("chunk #{} is not ready to consume",
                   nextChunkToConsume);

        currentChunk.getLock().lock();
        logger.debug("consumer get lock to check chunk state");

        while (currentChunk.getDownloadState() != DownloadState.SUCCESS &&
            currentChunk.getDownloadState() != DownloadState.FAILURE)
        {
          logger.debug("wait for chunk #{} to be ready, current"
                  + "chunk state is: {}",
              new Object[]{nextChunkToConsume, currentChunk.getDownloadState()});

          long startTime = System.currentTimeMillis();
          if(!currentChunk.getDownloadCondition().await(downloadedConditionTimeoutInSeconds, TimeUnit.SECONDS))
          {
            currentChunk.setDownloadState(DownloadState.FAILURE);
            currentChunk.setDownloadError(String.format("Timeout waiting for the download of chunk #%d" +
                "(Total chunks: %d)", nextChunkToConsume, this.chunks.size()));
          }
          this.numberMillisWaitingForChunks +=
              (System.currentTimeMillis() - startTime);

          logger.debug(
              "woken up from waiting for chunk #{} to be ready",
                     nextChunkToConsume);
        }

        // downloader thread encountered an error
        if (currentChunk.getDownloadState() == DownloadState.FAILURE)
        {
          logger.error("downloader encountered error: {}",
              currentChunk.getDownloadError());

          throw new SnowflakeSQLException(SqlState.INTERNAL_ERROR,
              ErrorCode.INTERNAL_ERROR.getMessageCode(),
              currentChunk.getDownloadError());
        }

        logger.debug("chunk #{} is ready to consume",
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
        if (terminateDownloader)
        {
          logger.debug("Download result fail. Shut down the chunk downloader");
          terminate();
        }
      }
    }
  }

  /**
   * terminate the downloader
   * @return chunk downloader metrics collected over instance lifetime
   */
  public Metrics terminate()
  {
    if (!terminated)
    {

      logger.debug("Total milliseconds waiting for chunks: {}, " +
              "Total memory used: {}, total download time: {} millisec, " +
              "total parsing time: {} milliseconds, total chunks: {}",
          numberMillisWaitingForChunks,
          Runtime.getRuntime().totalMemory(), totalMillisDownloadingChunks.get(),
          totalMillisParsingChunks.get(), chunks.size());

      if (executor != null)
      {
        executor.shutdownNow();
        executor = null;
      }
      chunks = null;
      chunkDataCache.clear();

      terminated = true;
      return new Metrics();
    }
    return null;
  }

  /**
   * add download time
   * @param downloadTime Time for downloading a single chunk
   */
  private void addDownloadTime(long downloadTime)
  {
    this.totalMillisDownloadingChunks.addAndGet(downloadTime);
  }

  /**
   * add parsing time
   * @param parsingTime Time for parsing a single chunk
   */
  private void addParsingTime(long parsingTime)
  {
    this.totalMillisParsingChunks.addAndGet(parsingTime);
  }

  /**
   * Create a download callable that will be run in download thread
   * @param downloader object to download the chunk
   * @param resultChunk object contains information about the chunk will
   *                    be downloaded
   * @param qrmk Query Result Master Key
   * @param chunkIndex the index of the chunk which will be downloaded in array
   *                   chunks. This is mainly for logging purpose
   * @param chunkHeadersMap contains headers needed to be added when downloading from s3
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
    return new Callable<Void> ()
    {
      public Void call() throws Exception
      {
        try
        {
          // set the chunk state to be in progress
          try
          {
            resultChunk.getLock().lock();
            resultChunk.setDownloadState(DownloadState.IN_PROGRESS);
          }
          finally
          {
            resultChunk.getLock().unlock();
          }

          logger.debug("Downloading chunk {}, url={}",
                                 new Object[]{chunkIndex, resultChunk.getUrl()});

          long startTime = System.currentTimeMillis();

          HttpResponse response = getResultChunk(resultChunk.getUrl());

          /*
           * return error if we don't get a response or the response code
           * means failure.
           */
          if (response == null
              || response.getStatusLine().getStatusCode() != 200)
          {
            logger.error( "Error fetching chunk from: {}",
                resultChunk.getUrl());

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

          InputStream jsonInputStream;
          final HttpEntity entity = response.getEntity();
          try
          {
            // read the chunk data
            InputStream is =
                  new HttpUtil.HttpInputStream(entity.getContent());

            // Determine the format of the response, if it is not
            // either plain text or gzip, raise an error.
            Header encoding = response.getFirstHeader("Content-Encoding");
            if (encoding != null)
            {
              if (encoding.getValue().equalsIgnoreCase("gzip"))
              {
                /* specify buffer size for GZIPInputStream */
                is = new GZIPInputStream(is, 65536);
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

            // Build a sequence of streams to wrap the input stream
            // with '[' ... ']' to be able to plug this in the
            // Jackson JSON parser.
            // gzip stream uses 64KB
            // no buffering as json parser does it internally
            jsonInputStream =
                new SequenceInputStream(
                    Collections.enumeration(Arrays.asList(
                        new ByteArrayInputStream("[".getBytes()),
                        is,
                        new ByteArrayInputStream("]".getBytes()))));
          }
          catch (Exception ex)
          {
            logger.error(
                       "Failed to uncompress data: {}",
                       response);

            throw ex;
          }

          // remember the download time
          resultChunk.setDownloadTime(System.currentTimeMillis() - startTime);
          downloader.addDownloadTime(resultChunk.getDownloadTime());

          startTime = System.currentTimeMillis();

          // trace the response if requested
          logger.debug("Json response: {}", response);

          JsonNode resultData = null;

          // parse the result json
          try
          {
            if (downloader.useJsonParser)
            {
              parseJsonToChunk(jsonInputStream,resultChunk);
            }
            else
            {
              // Use Jackson deserialization if not using JsonParser
              // tokenization.
              resultData = mapper.readTree(jsonInputStream);
            }
          }
          catch (Exception ex)
          {
            logger.error( "Exception when parsing result", ex);

            throw new SnowflakeSQLException(ex, SqlState.INTERNAL_ERROR,
                ErrorCode.INTERNAL_ERROR
                    .getMessageCode(),
                "Exception: " +
                    ex.getLocalizedMessage() +
                    "\nBad result json: " + response.toString());
          }
          finally
          {
            // close the buffer reader will close underlying stream
            jsonInputStream.close();
          }

          // add parsing time
          resultChunk.setParseTime(System.currentTimeMillis() - startTime);
          downloader.addParsingTime(resultChunk.getParseTime());

          // remember the result data (it can be null if using rowsets)
          resultChunk.setResultData(resultData);

          logger.debug(
                   "Finished preparing chunk data for {}, " +
                    "total download time={}ms, total parse time={}ms",
                     resultChunk.getUrl(),
                     resultChunk.getDownloadTime(),
                     resultChunk.getParseTime());

          try
          {
            resultChunk.getLock().lock();
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
                "Downloaded chunk {}, free lock", chunkIndex);

            resultChunk.getLock().unlock();
          }
        }
        catch (Throwable ex)
        {
          try
          {
            logger.debug("get lock to set chunk download error");
            resultChunk.getLock().lock();

            resultChunk.setDownloadState(DownloadState.FAILURE);
            resultChunk.setDownloadError(ex.getLocalizedMessage());

            logger.debug(
                "wake up consumer if it is waiting for a chunk to be ready");

            resultChunk.getDownloadCondition().signal();
          }
          finally
          {
            logger.debug("Failed to download chunk {}, free lock",
                chunkIndex);
            resultChunk.getLock().unlock();
          }

          logger.error(
                     "Exception encountered ({}:{}) fetching chunk from: {}",
                     new Object[]{
                         ex.getClass().getName(),
                         ex.getLocalizedMessage(),
                         resultChunk.getUrl()});

          logger.error( "Exception: ", ex);
        }

        return null;
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
            resultChunk.addRow(mapper.readValue(jp, Object[].class));
          }
          resultChunk.ensureRowsComplete();
        }
      }

      private HttpResponse getResultChunk(String chunkUrl) throws URISyntaxException, IOException, SnowflakeSQLException
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

        logger.debug("Fetching result: {}", resultChunk.getUrl());

        //TODO move this s3 request to HttpUtil class. In theory, upper layer
        //TODO does not need to know about http client
        CloseableHttpClient httpClient = HttpUtil.getHttpClient();

        // fetch the result chunk
        HttpResponse response =
            RestRequest.execute(httpClient,
                                httpRequest,
                                networkTimeoutInMilli / 1000, // retry timeout
                                0, // no socketime injection
                                null, // no canceling
                                false, // no cookie
                                false, // no retry
                                false); // no request_guid

        logger.debug("Call returned for URL: {}",
                               chunkUrl);
        return response;
      }
    };
  }
}
