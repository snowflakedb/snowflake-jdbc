/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import java.lang.ref.SoftReference;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.SqlState;

public class JsonResultChunk extends SnowflakeResultChunk {
  private static final int NULL_VALUE = Integer.MIN_VALUE;

  private static final SFLogger logger = SFLoggerFactory.getLogger(JsonResultChunk.class);

  private ResultChunkData data;

  private int currentRow;

  private SFBaseSession session;

  public JsonResultChunk(
      String url, int rowCount, int colCount, int uncompressedSize, SFBaseSession session) {
    super(url, rowCount, colCount, uncompressedSize);
    data = new BlockResultChunkDataV2(computeCharactersNeeded(), rowCount, colCount, session);
    this.session = session;
  }

  public static Object extractCell(JsonNode resultData, int rowIdx, int colIdx) {
    JsonNode currentRow = resultData.get(rowIdx);

    JsonNode colNode = currentRow.get(colIdx);

    if (colNode.isTextual()) {
      return colNode.asText();
    } else if (colNode.isNumber()) {
      return colNode.numberValue();
    } else if (colNode.isNull()) {
      return null;
    }
    throw new RuntimeException("Unknow json type");
  }

  public void tryReuse(ResultChunkDataCache cache) {
    // Allocate chunk data, double necessary amount for later reuse
    cache.reuseOrCreateResultData(data);
  }

  /**
   * Creates a String object for the given cell
   *
   * @param rowIdx zero based row
   * @param colIdx zero based column
   * @return String
   */
  public final Object getCell(int rowIdx, int colIdx) {
    return data.get(colCount * rowIdx + colIdx);
  }

  public final void addRow(Object[] row) throws SnowflakeSQLException {
    if (row.length != colCount) {
      throw new SnowflakeSQLLoggedException(
          this.session,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          SqlState.INTERNAL_ERROR,
          "Exception: expected " + colCount + " columns and received " + row.length);
    }

    for (Object cell : row) {
      if (cell == null) {
        data.add(null);
      } else {
        if (cell instanceof String) {
          data.add((String) cell);
        } else if (cell instanceof Boolean) {
          data.add((boolean) cell ? "1" : "0");
        } else {
          throw new SnowflakeSQLLoggedException(
              this.session,
              ErrorCode.INTERNAL_ERROR.getMessageCode(),
              SqlState.INTERNAL_ERROR,
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
  public final void ensureRowsComplete() throws SnowflakeSQLException {
    // Check that all the rows have been decoded, raise an error if not
    if (rowCount != currentRow) {
      throw new SnowflakeSQLException(
          SqlState.INTERNAL_ERROR,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          "Exception: expected " + rowCount + " rows and received " + currentRow);
    }
  }

  @Override
  public void reset() {
    this.currentRow = 0;
    this.data.reset();
  }

  /**
   * Compute the memory necessary to store the data of this chunk
   *
   * @return necessary memory in bytes
   */
  @Override
  public final long computeNeededChunkMemory() {
    if (data != null) {
      return data.computeNeededChunkMemory();
    }
    return 0;
  }

  @Override
  public final void freeData() {
    if (data != null) {
      data.freeData();
    }
  }

  public int computeCharactersNeeded() {
    // remove [ , ] characters, they won't be stored
    return uncompressedSize
        - (rowCount * 2) // opening [ and komma separating rows
        - (rowCount * colCount); // komma separating cells and closing ]
  }

  public void addOffset(int offset) throws SnowflakeSQLException {
    data.addOffset(offset);
  }

  public void setIsNull() throws SnowflakeSQLException {
    data.setIsNull();
  }

  public void setLastLength(int len) throws SnowflakeSQLException {
    data.setLastLength(len);
  }

  public void nextIndex() throws SnowflakeSQLException {
    data.nextIndex();
  }

  public byte get(int offset) throws SnowflakeSQLException {
    return data.getByte(offset);
  }

  public void addByte(byte b, int pos) throws SnowflakeSQLException {
    data.addByte(b, pos);
  }

  public void addBytes(byte[] src, int offset, int pos, int length) throws SnowflakeSQLException {
    data.addBytes(src, offset, pos, length);
  }

  /**
   * This class abstracts the storage of the strings in one chunk. To the user the class behaves
   * similar to an ArrayList.
   */
  private interface ResultChunkData {
    /**
     * Add the string to the data list
     *
     * @param string value to add
     */
    void add(String string) throws SnowflakeSQLException;

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

    /** Let GC collect the memory */
    void freeData();

    /**
     * add offset into offset buffer
     *
     * @param offset
     * @throws SnowflakeSQLException
     */
    void addOffset(int offset) throws SnowflakeSQLException;

    /**
     * label current value as null
     *
     * @throws SnowflakeSQLException
     */
    void setIsNull() throws SnowflakeSQLException;

    /**
     * increase index
     *
     * @throws SnowflakeSQLException
     */
    void nextIndex() throws SnowflakeSQLException;

    /**
     * set the last length
     *
     * @param len
     * @throws SnowflakeSQLException
     */
    void setLastLength(int len) throws SnowflakeSQLException;

    /**
     * get one byte from the byte array
     *
     * @param offset
     * @return
     * @throws SnowflakeSQLException
     */
    byte getByte(int offset) throws SnowflakeSQLException;

    /**
     * add one byte to the byte array at nextIndex
     *
     * @param b
     * @param pos
     * @throws SnowflakeSQLException
     */
    void addByte(byte b, int pos) throws SnowflakeSQLException;

    /**
     * add bytes to the byte array
     *
     * @param src
     * @param src_offset
     * @param pos
     * @param length
     * @throws SnowflakeSQLException
     */
    void addBytes(byte[] src, int src_offset, int pos, int length) throws SnowflakeSQLException;

    void reset();
  }

  /**
   * BlockResultChunkDataV2: This implementation copies the strings to byte arrays and stores the
   * offsets and bitmaps. This design can save half of the memory usage compared to the original one
   */
  private static class BlockResultChunkDataV2 implements ResultChunkData {
    BlockResultChunkDataV2(int totalLength, int rowCount, int colCount, SFBaseSession session) {
      this.blockCount = getBlock(totalLength - 1) + 1;
      this.rowCount = rowCount;
      this.colCount = colCount;
      this.metaBlockCount = getMetaBlock(this.rowCount * this.colCount - 1) + 1;
      this.session = session;
    }

    @Override
    public void reset() {
      freeData();
      this.lastLength = 0;
      this.nextIndex = 0;
    }

    @Override
    public void addOffset(int offset) {
      if (data.size() < blockCount || offsets.size() < metaBlockCount) {
        allocateArrays();
      }
      offsets.get(getMetaBlock(nextIndex))[getMetaBlockIndex(nextIndex)] = offset;
    }

    @Override
    public void setIsNull() {
      isNulls.get(getMetaBlock(nextIndex)).set(getMetaBlockIndex(nextIndex));
    }

    @Override
    public void setLastLength(int len) {
      lastLength = len;
    }

    @Override
    public byte getByte(int offset) {
      return data.get(getBlock(offset))[getBlockOffset(offset)];
    }

    @Override
    public void addByte(byte b, int pos) {
      if (data.size() < blockCount || offsets.size() < metaBlockCount) {
        allocateArrays();
      }
      data.get(getBlock(pos))[getBlockOffset(pos)] = b;
    }

    @Override
    public void addBytes(byte[] src, int src_offset, int pos, int length) {
      if (data.size() < blockCount || offsets.size() < metaBlockCount) {
        allocateArrays();
      }

      final int offset = pos;

      // copy string to the char array
      int copied = 0;
      if (spaceLeftOnBlock(offset) < length) {
        while (copied < length) {
          final int copySize = Math.min(length - copied, spaceLeftOnBlock(offset + copied));
          System.arraycopy(
              src,
              src_offset + copied,
              data.get(getBlock(offset + copied)),
              getBlockOffset(offset + copied),
              copySize);
          copied += copySize;
        }
      } else {
        System.arraycopy(
            src, src_offset, data.get(getBlock(offset)), getBlockOffset(offset), length);
      }
    }

    @Override
    public void nextIndex() {
      nextIndex++;
    }

    @Override
    public void add(String string) throws SnowflakeSQLException {
      throw new SnowflakeSQLLoggedException(
          this.session,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          SqlState.INTERNAL_ERROR,
          "Unimplemented");
    }

    private int getLength(int index, int offset) {
      if (index == rowCount * colCount - 1) {
        // last one
        return lastLength;
      } else {
        int nextOffset = offsets.get(getMetaBlock(index + 1))[getMetaBlockIndex(index + 1)];
        return nextOffset - offset;
      }
    }

    @Override
    public String get(int index) {
      final boolean isNull = isNulls.get(getMetaBlock(index)).get(getMetaBlockIndex(index));
      if (isNull) {
        return null;
      } else {
        final int offset = offsets.get(getMetaBlock(index))[getMetaBlockIndex(index)];
        final int length = getLength(index, offset);

        // Create string from the char arrays
        if (spaceLeftOnBlock(offset) < length) {
          int copied = 0;
          byte[] cell = new byte[length];
          while (copied < length) {
            final int copySize = Math.min(length - copied, spaceLeftOnBlock(offset + copied));
            System.arraycopy(
                data.get(getBlock(offset + copied)),
                getBlockOffset(offset + copied),
                cell,
                copied,
                copySize);

            copied += copySize;
          }
          return new String(cell, StandardCharsets.UTF_8);
        } else {
          return new String(
              data.get(getBlock(offset)), getBlockOffset(offset), length, StandardCharsets.UTF_8);
        }
      }
    }

    @Override
    public long computeNeededChunkMemory() {
      long dataRequirement = blockCount * blockLength * 1L;
      long metadataRequirement =
          metaBlockCount * metaBlockLength * 4L // offsets
              + metaBlockCount * metaBlockLength / 8L // isNulls
              + 1L; // lastLength

      return dataRequirement + metadataRequirement;
    }

    @Override
    public void freeData() {
      data.clear();
      offsets.clear();
      isNulls.clear();
    }

    private static int getBlock(int offset) {
      return offset >> blockLengthBits;
    }

    private static int getBlockOffset(int offset) {
      return offset & (blockLength - 1);
    }

    private static int spaceLeftOnBlock(int offset) {
      return blockLength - getBlockOffset(offset);
    }

    private static int getMetaBlock(int index) {
      return index >> metaBlockLengthBits;
    }

    private static int getMetaBlockIndex(int index) {
      return index & (metaBlockLength - 1);
    }

    private void allocateArrays() {
      logger.debug("allocating {} B for ResultChunk", computeNeededChunkMemory());
      while (data.size() < blockCount) {
        data.add(new byte[1 << blockLengthBits]);
      }
      while (offsets.size() < metaBlockCount) {
        offsets.add(new int[1 << metaBlockLengthBits]);
        isNulls.add(new BitSet(1 << metaBlockLengthBits));
      }
      logger.debug("allocated {} B for ResultChunk", computeNeededChunkMemory());
    }

    // blocks for storing the string data
    int blockCount;
    private static final int blockLengthBits = 23;
    private static int blockLength = 1 << blockLengthBits;
    private final ArrayList<byte[]> data = new ArrayList<>();
    SFBaseSession session;

    // blocks for storing offsets and lengths
    int metaBlockCount;
    private static int metaBlockLengthBits = 15;
    private static int metaBlockLength = 1 << metaBlockLengthBits;
    private final ArrayList<int[]> offsets = new ArrayList<>();
    private final ArrayList<BitSet> isNulls = new ArrayList<>();
    private int lastLength;
    private int rowCount, colCount;
    private int nextIndex = 0;
  }

  /** Cache the data, offset and length blocks */
  static class ResultChunkDataCache {
    /**
     * Add the data to the cache. CAUTION: The result chunk is not usable afterward
     *
     * @param chunk add this to the cache
     */
    void add(JsonResultChunk chunk) {
      cache.add(new SoftReference<>(chunk.data));
      chunk.data = null;
    }

    /**
     * Creates a new ResultChunkData which reuses as much blocks as possible
     *
     * @param data fill this with reused blocks
     */
    void reuseOrCreateResultData(ResultChunkData data) {
      List<SoftReference<ResultChunkData>> remove = new ArrayList<>();
      try {
        for (SoftReference<ResultChunkData> ref : cache) {
          ResultChunkData dat = ref.get();
          if (dat == null) {
            remove.add(ref);
            continue;
          }
          if (dat instanceof BlockResultChunkDataV2) {
            BlockResultChunkDataV2 bTargetData = (BlockResultChunkDataV2) data;
            BlockResultChunkDataV2 bCachedDat = (BlockResultChunkDataV2) dat;
            if (bCachedDat.data.size() == 0 && bCachedDat.offsets.size() == 0) {
              remove.add(ref);
              continue;
            }

            while (bTargetData.data.size() < bTargetData.blockCount && bCachedDat.data.size() > 0) {
              bTargetData.data.add(bCachedDat.data.remove(bCachedDat.data.size() - 1));
            }
            while (bTargetData.offsets.size() < bTargetData.metaBlockCount
                && bCachedDat.offsets.size() > 0) {
              bTargetData.offsets.add(bCachedDat.offsets.remove(bCachedDat.offsets.size() - 1));
              BitSet isNulls = bCachedDat.isNulls.remove(bCachedDat.isNulls.size() - 1);
              isNulls.clear(); // SNOW-80208 have to clear isNulls explicitly
              bTargetData.isNulls.add(isNulls);
            }
            if (bTargetData.data.size() == bTargetData.blockCount
                && bTargetData.offsets.size() == bTargetData.metaBlockCount) {
              return;
            }
          } else {
            remove.add(ref);
          }
        }
      } finally {
        cache.removeAll(remove);
      }
    }

    /** Let GC collect all data hold by the cache */
    void clear() {
      for (SoftReference<ResultChunkData> ref : cache) {
        ResultChunkData dat = ref.get();
        if (dat != null) {
          dat.freeData();
        }
      }
      cache.clear();
    }

    List<SoftReference<ResultChunkData>> cache = new LinkedList<>();
  }
}
