/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import net.snowflake.common.core.SqlState;

import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Class for result chunk
 * <p>
 * Created by jhuang on 11/12/14.
 */
public class SnowflakeResultChunk
{
  private static final int NULL_VALUE = Integer.MIN_VALUE;

  public enum DownloadState
  {
    NOT_STARTED,
    IN_PROGRESS,
    SUCCESS,
    FAILURE
  }

  ;

  // url for result chunk
  private final String url;

  // number of columns to expect
  private final int colCount;

  // uncompressed size in bytes of this chunk
  private int uncompressedSize;

  // row count
  private final int rowCount;

  // result data
  private volatile JsonNode resultData;

  private ResultChunkData data;

  // download time for the chunk
  private long downloadTime;

  // parse time for the chunk
  private long parseTime;

  private DownloadState downloadState = DownloadState.NOT_STARTED;

  // lock for guarding shared chunk state between consumer and downloader
  private ReentrantLock lock = new ReentrantLock();

  // a condition to signal from downloader to consumer
  private Condition downloadCondition = lock.newCondition();

  // download error if any for the chunk
  private String downloadError;

  private int currentRow;

  public SnowflakeResultChunk(String url, int rowCount, int colCount,
                              int uncompressedSize, boolean efficientStorage)
  {
    this.url = url;
    this.rowCount = rowCount;
    this.colCount = colCount;
    this.uncompressedSize = uncompressedSize;
    if (efficientStorage)
    {
      data = new BlockResultChunkData(computeCharactersNeeded(),
                                      rowCount * colCount);
    }
    else
    {
      data = new LegacyResultChunkData(computeCharactersNeeded(),
                                       rowCount * colCount);
    }
  }

  public void tryReuse(ResultChunkDataCache cache)
  {
    // Allocate chunk data, double necessary amount for later reuse
    cache.reuseOrCreateResultData(data);
  }

  /**
   * Set the data when using the fasterxml json parser
   *
   * @param resultData result data in JSON form
   */
  @Deprecated
  public void setResultData(JsonNode resultData)
  {
    this.resultData = resultData;
  }

  public static Object extractCell(JsonNode resultData, int rowIdx, int colIdx)
  {
    JsonNode currentRow = resultData.get(rowIdx);

    JsonNode colNode = currentRow.get(colIdx);

    if (colNode.isTextual())
    {
      return colNode.asText();
    }
    else if (colNode.isNumber())
    {
      return colNode.numberValue();
    }
    else if (colNode.isNull())
    {
      return null;
    }
    throw new RuntimeException("Unknow json type");
  }

  /**
   * Creates a String object for the given cell
   *
   * @param rowIdx zero based row
   * @param colIdx zero based column
   * @return String
   */
  public final Object getCell(int rowIdx, int colIdx)
  {
    if (resultData != null)
    {
      return extractCell(resultData, rowIdx, colIdx);
    }
    return data.get(colCount * rowIdx + colIdx);
  }

  public final String getUrl()
  {
    return url;
  }

  public final int getRowCount()
  {
    return rowCount;
  }

  public final int getUncompressedSize()
  {
    return uncompressedSize;
  }

  public final void addRow(Object[] row) throws SnowflakeSQLException
  {
    if (row.length != colCount)
    {
      throw new SnowflakeSQLException(
          SqlState.INTERNAL_ERROR,
          ErrorCode.INTERNAL_ERROR
              .getMessageCode(),
          "Exception: expected " +
          colCount +
          " columns and received " +
          row.length);
    }

    for (Object cell : row)
    {
      if (cell == null)
      {
        data.add(null);
      }
      else
      {
        if (cell instanceof String)
        {
          data.add((String) cell);
        }
        else if (cell instanceof Boolean)
        {
          data.add((boolean) cell ? "1" : "0");
        }
        else
        {
          throw new SnowflakeSQLException(
              SqlState.INTERNAL_ERROR,
              ErrorCode.INTERNAL_ERROR.getMessageCode(),
              "unknown data type in JSON row " + cell.getClass().toString());
        }
      }
    }
    currentRow++;
  }

  /**
   * Checks that all data has been added after parsing.
   *
   * @throws SnowflakeSQLException when rows are not all downloaded
   */
  public final void ensureRowsComplete() throws SnowflakeSQLException
  {
    // Check that all the rows have been decoded, raise an error if not
    if (rowCount != currentRow)
    {
      throw
          new SnowflakeSQLException(
              SqlState.INTERNAL_ERROR,
              ErrorCode.INTERNAL_ERROR
                  .getMessageCode(),
              "Exception: expected " +
              rowCount +
              " rows and received " +
              currentRow);
    }
  }

  /**
   * Compute the memory necessary to store the data of this chunk
   *
   * @return necessary memory in bytes
   */
  public final long computeNeededChunkMemory()
  {
    return data.computeNeededChunkMemory();
  }

  public final void freeData()
  {
    if (data != null)
    {
      data.freeData();
    }
    resultData = null;
  }

  public final int getColCount()
  {
    return this.colCount;
  }

  public long getDownloadTime()
  {
    return downloadTime;
  }

  public void setDownloadTime(long downloadTime)
  {
    this.downloadTime = downloadTime;
  }

  public long getParseTime()
  {
    return parseTime;
  }

  public void setParseTime(long parseTime)
  {
    this.parseTime = parseTime;
  }

  public ReentrantLock getLock()
  {
    return lock;
  }

  public Condition getDownloadCondition()
  {
    return downloadCondition;
  }

  public String getDownloadError()
  {
    return downloadError;
  }

  public void setDownloadError(String downloadError)
  {
    this.downloadError = downloadError;
  }

  public DownloadState getDownloadState()
  {
    return downloadState;
  }

  public void setDownloadState(DownloadState downloadState)
  {
    this.downloadState = downloadState;
  }

  private int computeCharactersNeeded()
  {
    // remove [ , ] characters, they won't be stored
    return uncompressedSize
           - (rowCount * 2) // opening [ and komma separating rows
           - (rowCount * colCount); // komma separating cells and closing ]
  }

  /**
   * This class abstracts the storage of the strings in one chunk.
   * To the user the class behaves similar to an ArrayList.
   */
  private static interface ResultChunkData
  {
    /**
     * Add the string to the data list
     *
     * @param string value to add
     */
    void add(String string);

    /**
     * Access an element by an index
     *
     * @param index determines the element
     * @return String containing the same data as the one passed to add()
     */
    String get(int index);

    /**
     * Compute the necessary memory to store this chunk
     *
     * @return memory in bytes
     */
    long computeNeededChunkMemory();

    /**
     * Let GC collect the memory
     */
    void freeData();
  }

  /**
   * This implementation copies the strings to char arrays and stores the
   * offsets and lengths.
   * Multiple smaller arrays are necessary because java is not good at
   * handling big arrays. They can cause OOM even if there is enough heap space
   * left in theory.
   */
  private static class BlockResultChunkData implements ResultChunkData
  {
    BlockResultChunkData(int totalLength, int count)
    {
      this.blockCount = getBlock(totalLength - 1) + 1;
      this.metaBlockCount = getMetaBlock(count - 1) + 1;
    }

    @Override
    public void add(String string)
    {
      if (data.size() < blockCount || offsets.size() < metaBlockCount)
      {
        allocateArrays();
      }

      if (string == null)
      {
        lengths.get(getMetaBlock(nextIndex))
            [getMetaBlockIndex(nextIndex)] = NULL_VALUE;
      }
      else
      {
        final int offset = currentDatOffset;
        final int length = string.length();

        // store offset and length
        offsets.get(getMetaBlock(nextIndex))
            [getMetaBlockIndex(nextIndex)] = offset;
        lengths.get(getMetaBlock(nextIndex))
            [getMetaBlockIndex(nextIndex)] = length;

        // copy string to the char array
        int copied = 0;
        char[] source = string.toCharArray();
        if (spaceLeftOnBlock(offset) < length)
        {
          while (copied < length)
          {
            final int copySize
                = Math.min(length - copied, spaceLeftOnBlock(offset + copied));
            System.arraycopy(source, copied,
                             data.get(getBlock(offset + copied)),
                             getBlockOffset(offset + copied),
                             copySize);
            copied += copySize;
          }
        }
        else
        {
          System.arraycopy(source, 0,
                           data.get(getBlock(offset)),
                           getBlockOffset(offset), string.length());
        }
        currentDatOffset += string.length();
      }
      nextIndex++;
    }

    @Override
    public String get(int index)
    {
      final int length = lengths.get(getMetaBlock(index))
          [getMetaBlockIndex(index)];
      if (length == NULL_VALUE)
      {
        return null;
      }
      else
      {
        final int offset = offsets.get(getMetaBlock(index))
            [getMetaBlockIndex(index)];

        // Create string from the char arrays
        if (spaceLeftOnBlock(offset) < length)
        {
          int copied = 0;
          char[] cell = new char[length];
          while (copied < length)
          {
            final int copySize
                = Math.min(length - copied, spaceLeftOnBlock(offset + copied));
            System.arraycopy(data.get(getBlock(offset + copied)),
                             getBlockOffset(offset + copied),
                             cell, copied,
                             copySize);

            copied += copySize;
          }
          return new String(cell);
        }
        else
        {
          return new String(data.get(getBlock(offset)),
                            getBlockOffset(offset),
                            length);
        }
      }
    }

    @Override
    public long computeNeededChunkMemory()
    {
      long dataRequirement = blockCount * blockLength * 2L;
      long metadataRequirement = metaBlockCount * metaBlockLength * (4L + 4L);

      return dataRequirement + metadataRequirement;
    }

    @Override
    public void freeData()
    {
      data.clear();
      offsets.clear();
      lengths.clear();
    }

    private static int getBlock(int offset)
    {
      return offset >> blockLengthBits;
    }

    private static int getBlockOffset(int offset)
    {
      return offset & (blockLength - 1);
    }

    private static int spaceLeftOnBlock(int offset)
    {
      return blockLength - getBlockOffset(offset);
    }

    private static int getMetaBlock(int index)
    {
      return index >> metaBlockLengthBits;
    }

    private static int getMetaBlockIndex(int index)
    {
      return index & (metaBlockLength - 1);
    }

    private void allocateArrays()
    {
      while (data.size() < blockCount)
      {
        data.add(new char[1 << blockLengthBits]);
      }
      while (offsets.size() < metaBlockCount)
      {
        offsets.add(new int[1 << metaBlockLengthBits]);
        lengths.add(new int[1 << metaBlockLengthBits]);
      }
    }

    // blocks for storing the string data
    int blockCount;
    private static final int blockLengthBits = 24;
    private static int blockLength = 1 << blockLengthBits;
    private final ArrayList<char[]> data = new ArrayList<>();
    private int currentDatOffset = 0;

    // blocks for storing offsets and lengths
    int metaBlockCount;
    private static int metaBlockLengthBits = 15;
    private static int metaBlockLength = 1 << metaBlockLengthBits;
    private final ArrayList<int[]> offsets = new ArrayList<>();
    private final ArrayList<int[]> lengths = new ArrayList<>();
    private int nextIndex = 0;
  }

  private static class LegacyResultChunkData implements ResultChunkData
  {
    private final int totalLength, count;
    ArrayList<String> list;

    LegacyResultChunkData(int totalLength, int count)
    {
      this.totalLength = totalLength;
      this.count = count;
    }

    @Override
    public void add(String string)
    {
      if (list == null)
      {
        list = new ArrayList<>(count);
      }
      list.add(string);
    }

    @Override
    public String get(int index)
    {
      return list.get(index);
    }

    @Override
    public long computeNeededChunkMemory()
    {
      return totalLength + 8 * count + 45 * count;
    }

    @Override
    public void freeData()
    {
      if (list != null)
      {
        list.clear();
        list.trimToSize();
      }
    }
  }

  /**
   * Cache the data, offset and length blocks
   */
  static class ResultChunkDataCache
  {
    /**
     * Add the data to the cache.
     * CAUTION: The result chunk is not usable afterward
     *
     * @param chunk add this to the cache
     */
    void add(SnowflakeResultChunk chunk)
    {
      cache.add(new SoftReference<>(chunk.data));
      chunk.data = null;
    }

    /**
     * Creates a new ResultChunkData which reuses as much blocks as possible
     *
     * @param data fill this with reused blocks
     */
    void reuseOrCreateResultData(ResultChunkData data)
    {
      List<SoftReference<ResultChunkData>> remove = new ArrayList<>();
      try
      {
        for (SoftReference<ResultChunkData> ref : cache)
        {
          ResultChunkData dat = ref.get();
          if (dat == null)
          {
            remove.add(ref);
            continue;
          }
          if (dat instanceof BlockResultChunkData)
          {
            BlockResultChunkData bTargetData = (BlockResultChunkData) data;
            BlockResultChunkData bCachedDat = (BlockResultChunkData) dat;
            if (bCachedDat.data.size() == 0 && bCachedDat.offsets.size() == 0)
            {
              remove.add(ref);
              continue;
            }

            while (bTargetData.data.size() < bTargetData.blockCount && bCachedDat.data.size() > 0)
            {
              bTargetData.data.add(bCachedDat.data.remove(bCachedDat.data.size() - 1));
            }
            while (bTargetData.offsets.size() < bTargetData.metaBlockCount && bCachedDat.offsets.size() > 0)
            {
              bTargetData.offsets.add(bCachedDat.offsets.remove(bCachedDat.offsets.size() - 1));
              bTargetData.lengths.add(bCachedDat.lengths.remove(bCachedDat.lengths.size() - 1));
            }
            if (bTargetData.data.size() == bTargetData.blockCount &&
                bTargetData.offsets.size() == bTargetData.metaBlockCount)
            {
              return;
            }
          }
          else if (dat instanceof LegacyResultChunkData)
          {
            LegacyResultChunkData lTargetData = (LegacyResultChunkData) data;
            LegacyResultChunkData lCachedDat = (LegacyResultChunkData) dat;

            if (lCachedDat.list == null)
            {
              remove.add(ref);
              continue;
            }
            if (lTargetData.list == null)
            {
              lTargetData.list = lCachedDat.list;
              lTargetData.list.clear();
              lCachedDat.list = null;
              remove.add(ref);
              return;
            }
          }
          else
          {
            remove.add(ref);
          }
        }
      }
      finally
      {
        cache.removeAll(remove);
      }
    }

    /**
     * Let GC collect all data hold by the cache
     */
    void clear()
    {
      for (SoftReference<ResultChunkData> ref : cache)
      {
        ResultChunkData dat = ref.get();
        if (dat != null)
        {
          dat.freeData();
        }
      }
      cache.clear();
    }

    List<SoftReference<ResultChunkData>> cache = new LinkedList<>();
  }
}
