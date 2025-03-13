package net.snowflake.client.jdbc;

import static net.snowflake.client.core.arrow.ArrowVectorConverterUtil.initConverter;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.ClosedByInterruptException;
import java.util.ArrayList;
import java.util.List;
import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.SFBaseSession;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.arrow.ArrowResultChunkIndexSorter;
import net.snowflake.client.core.arrow.ArrowVectorConverter;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import net.snowflake.common.core.SqlState;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.util.TransferPair;

public class ArrowResultChunk extends SnowflakeResultChunk {
  /**
   * A 2-D array of arrow ValueVectors, this list represents data in the whole chunk. Since each
   * chunk is divided into record batches and each record batch is composed of list of column
   * vectors.
   *
   * <p>So the outer list is list of record batches, inner list represents list of columns
   */
  private final ArrayList<List<ValueVector>> batchOfVectors;

  private static final SFLogger logger = SFLoggerFactory.getLogger(ArrowResultChunk.class);

  /** arrow root allocator used by this resultSet */
  private final RootAllocator rootAllocator;

  private boolean enableSortFirstResultChunk;
  private IntVector firstResultChunkSortedIndices;
  private VectorSchemaRoot root;
  private SFBaseSession session;

  public ArrowResultChunk(
      String url,
      int rowCount,
      int colCount,
      int uncompressedSize,
      RootAllocator rootAllocator,
      SFBaseSession session) {
    super(url, rowCount, colCount, uncompressedSize);
    this.batchOfVectors = new ArrayList<>();
    this.rootAllocator = rootAllocator;
    this.session = session;
  }

  private void addBatchData(List<ValueVector> batch) {
    batchOfVectors.add(batch);
  }

  /**
   * Read an inputStream of arrow data bytes and load them into java vectors of value. Note, there
   * is no copy of data involved once data is loaded into memory. a.k.a ArrowStreamReader originally
   * allocates the memory to hold vectors, but those memory ownership is transfer into
   * ArrowResultChunk class and once ArrowStreamReader is garbage collected, memory will not be
   * cleared up
   *
   * @param is inputStream which contains arrow data file in bytes
   * @throws IOException if failed to read data as arrow file
   */
  public void readArrowStream(InputStream is) throws IOException {
    ArrayList<ValueVector> valueVectors = new ArrayList<>();
    try (ArrowStreamReader reader = new ArrowStreamReader(is, rootAllocator)) {
      root = reader.getVectorSchemaRoot();
      while (reader.loadNextBatch()) {
        valueVectors = new ArrayList<>();

        for (FieldVector f : root.getFieldVectors()) {
          // transfer will not copy data but transfer ownership of memory
          // from streamReader to resultChunk
          TransferPair t = f.getTransferPair(rootAllocator);
          t.transfer();
          valueVectors.add(t.getTo());
        }

        addBatchData(valueVectors);
        root.clear();
      }
    } catch (ClosedByInterruptException cbie) {
      // happens when the statement is closed before finish parsing
      logger.debug("Interrupted when loading Arrow result", cbie);
      valueVectors.forEach(ValueVector::close);
      freeData();
    } catch (Exception ex) {
      valueVectors.forEach(ValueVector::close);
      freeData();
      throw ex;
    }
  }

  @Override
  public void reset() {
    freeData();
    this.batchOfVectors.clear();
  }

  @Override
  public long computeNeededChunkMemory() {
    return getUncompressedSize();
  }

  @Override
  public void freeData() {
    batchOfVectors.forEach(list -> list.forEach(ValueVector::close));
    this.batchOfVectors.clear();
    if (firstResultChunkSortedIndices != null) {
      firstResultChunkSortedIndices.close();
    }
    if (root != null) {
      root.clear();
      root = null;
    }
  }

  /**
   * @param dataConversionContext DataConversionContext
   * @return an iterator to iterate over current chunk
   */
  public ArrowChunkIterator getIterator(DataConversionContext dataConversionContext) {
    return new ArrowChunkIterator(dataConversionContext);
  }

  /**
   * @return an empty iterator to iterate over current chunk
   */
  public static ArrowChunkIterator getEmptyChunkIterator() {
    return new EmptyArrowResultChunk().new ArrowChunkIterator(null);
  }

  public void enableSortFirstResultChunk() {
    enableSortFirstResultChunk = true;
  }

  /** Iterator class used to go through the arrow chunk row by row */
  public class ArrowChunkIterator {
    /** index of record batch that iterator currently points to */
    private int currentRecordBatchIndex;

    /** total number of record batch */
    private int totalRecordBatch;

    /** index of row inside current record batch that iterator points to */
    private int currentRowInRecordBatch;

    /** number of rows inside current record batch */
    private int rowCountInCurrentRecordBatch;

    /**
     * list of converters that attached to current record batch Note: this list is updated every
     * time iterator points to a new record batch
     */
    private List<ArrowVectorConverter> currentConverters;

    /** formatters to each data type */
    private DataConversionContext dataConversionContext;

    ArrowChunkIterator(DataConversionContext dataConversionContext) {
      this.currentRecordBatchIndex = -1;
      this.totalRecordBatch = batchOfVectors.size();
      this.currentRowInRecordBatch = -1;
      this.rowCountInCurrentRecordBatch = 0;
      this.dataConversionContext = dataConversionContext;
    }

    /**
     * Given a list of arrow vectors (all columns in a single record batch), return list of arrow
     * vector converter. Note, converter is built on top of arrow vector, so that arrow data can be
     * converted back to java data
     *
     * @param vectors list of arrow vectors
     * @return list of converters on top of each converters
     */
    private List<ArrowVectorConverter> initConverters(List<ValueVector> vectors)
        throws SnowflakeSQLException {
      List<ArrowVectorConverter> converters = new ArrayList<>();
      for (int i = 0; i < vectors.size(); i++) {
        converters.add(initConverter(vectors.get(i), dataConversionContext, session, i));
      }
      return converters;
    }

    /**
     * Advance to next row.
     *
     * @return true if there is a next row
     * @throws SnowflakeSQLException if an error is encountered.
     */
    public boolean next() throws SnowflakeSQLException {
      currentRowInRecordBatch++;
      if (currentRowInRecordBatch < rowCountInCurrentRecordBatch) {
        // still in current recordbatch
        return true;
      } else {
        currentRecordBatchIndex++;
        if (currentRecordBatchIndex < totalRecordBatch) {
          this.currentRowInRecordBatch = 0;
          if (currentRecordBatchIndex == 0 && sortFirstResultChunkEnabled()) {
            // perform client-side sorting for the first chunk (only used in Snowflake internal
            // regression tests)
            // if first chunk has multiple record batches, merge them into one and sort it
            if (batchOfVectors.size() > 1) {
              mergeBatchesIntoOne();
              totalRecordBatch = 1;
            }
            this.rowCountInCurrentRecordBatch =
                batchOfVectors.get(currentRecordBatchIndex).get(0).getValueCount();
            currentConverters = initConverters(batchOfVectors.get(currentRecordBatchIndex));
            sortFirstResultChunk(currentConverters);
          } else {
            this.rowCountInCurrentRecordBatch =
                batchOfVectors.get(currentRecordBatchIndex).get(0).getValueCount();
            currentConverters = initConverters(batchOfVectors.get(currentRecordBatchIndex));
          }
          return true;
        }
      }
      return false;
    }

    public boolean isLast() {
      return currentRecordBatchIndex + 1 == totalRecordBatch
          && currentRowInRecordBatch + 1 == rowCountInCurrentRecordBatch;
    }

    public boolean isAfterLast() {
      return currentRecordBatchIndex >= totalRecordBatch
          && currentRowInRecordBatch >= rowCountInCurrentRecordBatch;
    }

    public ArrowResultChunk getChunk() {
      return ArrowResultChunk.this;
    }

    public ArrowVectorConverter getCurrentConverter(int columnIdx) throws SFException {
      if (columnIdx < 0 || columnIdx >= currentConverters.size()) {
        throw new SFException(ErrorCode.COLUMN_DOES_NOT_EXIST, columnIdx + 1);
      }

      return currentConverters.get(columnIdx);
    }

    /**
     * @return index of row in current record batch
     */
    public int getCurrentRowInRecordBatch() {
      if (sortFirstResultChunkEnabled() && currentRecordBatchIndex == 0) {
        return firstResultChunkSortedIndices.get(currentRowInRecordBatch);
      } else {
        return currentRowInRecordBatch;
      }
    }
  }

  /**
   * merge arrow result chunk with more than one batches into one record batch (Only used for the
   * first chunk when client side sorting is required)
   *
   * @throws SnowflakeSQLException if failed to merge first result chunk
   */
  public void mergeBatchesIntoOne() throws SnowflakeSQLException {
    try {
      List<ValueVector> first = batchOfVectors.get(0);
      for (int i = 1; i < batchOfVectors.size(); i++) {
        List<ValueVector> batch = batchOfVectors.get(i);
        mergeBatch(first, batch);
        batch.forEach(ValueVector::close);
      }
      batchOfVectors.clear();
      batchOfVectors.add(first);
    } catch (SFException ex) {
      throw new SnowflakeSQLLoggedException(
          session,
          SqlState.INTERNAL_ERROR,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          ex,
          "Failed to merge first result chunk: " + ex.getLocalizedMessage());
    }
  }

  /**
   * merge right batch into the left batch
   *
   * @param left
   * @param right
   */
  private void mergeBatch(List<ValueVector> left, List<ValueVector> right) throws SFException {
    for (int i = 0; i < left.size(); i++) {
      mergeVector(left.get(i), right.get(i));
    }
  }

  /**
   * todo append values from the right vector to the left
   *
   * @param left
   * @param right
   */
  private void mergeVector(ValueVector left, ValueVector right) throws SFException {
    if (left instanceof StructVector) {
      mergeStructVector((StructVector) left, (StructVector) right);
    } else {
      mergeNonStructVector(left, right);
    }
  }

  /**
   * TODO merge StructVector used by Snowflake timestamp types
   *
   * @param left
   * @param right
   */
  private void mergeStructVector(StructVector left, StructVector right) throws SFException {
    int numOfChildren = left.getChildrenFromFields().size();
    for (int i = 0; i < numOfChildren; i++) {
      mergeNonStructVector(
          left.getChildrenFromFields().get(i), right.getChildrenFromFields().get(i));
    }
    int offset = left.getValueCount();
    for (int i = 0; i < right.getValueCount(); i++) {
      if (right.isNull(i)) {
        left.setNull(offset + i);
      }
    }
    left.setValueCount(offset + right.getValueCount());
  }

  /**
   * merge not struct vectors
   *
   * @param left
   * @param right
   */
  private void mergeNonStructVector(ValueVector left, ValueVector right) throws SFException {
    if (left instanceof BigIntVector) {
      BigIntVector bigIntVectorLeft = (BigIntVector) left;
      BigIntVector bigIntVectorRight = (BigIntVector) right;
      int offset = bigIntVectorLeft.getValueCount();
      for (int i = 0; i < bigIntVectorRight.getValueCount(); i++) {
        if (bigIntVectorRight.isNull(i)) {
          bigIntVectorLeft.setNull(offset + i);
        } else {
          bigIntVectorLeft.setSafe(offset + i, bigIntVectorRight.get(i));
        }
      }
      bigIntVectorLeft.setValueCount(offset + bigIntVectorRight.getValueCount());
    } else if (left instanceof BitVector) {
      BitVector bitVectorLeft = (BitVector) left;
      BitVector bitVectorRight = (BitVector) right;
      int offset = bitVectorLeft.getValueCount();
      for (int i = 0; i < bitVectorRight.getValueCount(); i++) {
        if (bitVectorRight.isNull(i)) {
          bitVectorLeft.setNull(offset + i);
        } else {
          try {
            bitVectorLeft.setSafe(offset + i, bitVectorRight.get(i));
          } catch (IndexOutOfBoundsException e) {
            // this can be a bug in arrow that doesn't safely set value for
            // BitVector so we have to reAlloc manually
            bitVectorLeft.reAlloc();
            bitVectorLeft.setSafe(offset + i, bitVectorRight.get(i));
          }
        }
      }
      bitVectorLeft.setValueCount(offset + bitVectorRight.getValueCount());
    } else if (left instanceof DateDayVector) {
      DateDayVector dateDayVectorLeft = (DateDayVector) left;
      DateDayVector dateDayVectorRight = (DateDayVector) right;
      int offset = dateDayVectorLeft.getValueCount();
      for (int i = 0; i < dateDayVectorRight.getValueCount(); i++) {
        if (dateDayVectorRight.isNull(i)) {
          dateDayVectorLeft.setNull(offset + i);
        } else {
          dateDayVectorLeft.setSafe(offset + i, dateDayVectorRight.get(i));
        }
      }
      dateDayVectorLeft.setValueCount(offset + dateDayVectorRight.getValueCount());
    } else if (left instanceof DecimalVector) {
      DecimalVector decimalVectorLeft = (DecimalVector) left;
      DecimalVector decimalVectorRight = (DecimalVector) right;
      int offset = decimalVectorLeft.getValueCount();
      for (int i = 0; i < decimalVectorRight.getValueCount(); i++) {
        if (decimalVectorRight.isNull(i)) {
          decimalVectorLeft.setNull(offset + i);
        } else {
          decimalVectorLeft.setSafe(offset + i, decimalVectorRight.get(i));
        }
      }
      decimalVectorLeft.setValueCount(offset + decimalVectorRight.getValueCount());
    } else if (left instanceof Float8Vector) {
      Float8Vector float8VectorLeft = (Float8Vector) left;
      Float8Vector float8VectorRight = (Float8Vector) right;
      int offset = float8VectorLeft.getValueCount();
      for (int i = 0; i < float8VectorRight.getValueCount(); i++) {
        if (float8VectorRight.isNull(i)) {
          float8VectorLeft.setNull(offset + i);
        } else {
          float8VectorLeft.setSafe(offset + i, float8VectorRight.get(i));
        }
      }
      float8VectorLeft.setValueCount(offset + float8VectorRight.getValueCount());
    } else if (left instanceof IntVector) {
      IntVector intVectorLeft = (IntVector) left;
      IntVector intVectorRight = (IntVector) right;
      int offset = intVectorLeft.getValueCount();
      for (int i = 0; i < intVectorRight.getValueCount(); i++) {
        if (intVectorRight.isNull(i)) {
          intVectorLeft.setNull(offset + i);
        } else {
          intVectorLeft.setSafe(offset + i, intVectorRight.get(i));
        }
      }
      intVectorLeft.setValueCount(offset + intVectorRight.getValueCount());
    } else if (left instanceof SmallIntVector) {
      SmallIntVector smallIntVectorLeft = (SmallIntVector) left;
      SmallIntVector smallIntVectorRight = (SmallIntVector) right;
      int offset = smallIntVectorLeft.getValueCount();
      for (int i = 0; i < smallIntVectorRight.getValueCount(); i++) {
        if (smallIntVectorRight.isNull(i)) {
          smallIntVectorLeft.setNull(offset + i);
        } else {
          smallIntVectorLeft.setSafe(offset + i, smallIntVectorRight.get(i));
        }
      }
      smallIntVectorLeft.setValueCount(offset + smallIntVectorRight.getValueCount());
    } else if (left instanceof TinyIntVector) {
      TinyIntVector tinyIntVectorLeft = (TinyIntVector) left;
      TinyIntVector tinyIntVectorRight = (TinyIntVector) right;
      int offset = tinyIntVectorLeft.getValueCount();
      for (int i = 0; i < tinyIntVectorRight.getValueCount(); i++) {
        if (tinyIntVectorRight.isNull(i)) {
          tinyIntVectorLeft.setNull(offset + i);
        } else {
          tinyIntVectorLeft.setSafe(offset + i, tinyIntVectorRight.get(i));
        }
      }
      tinyIntVectorLeft.setValueCount(offset + tinyIntVectorRight.getValueCount());
    } else if (left instanceof VarBinaryVector) {
      VarBinaryVector varBinaryVectorLeft = (VarBinaryVector) left;
      VarBinaryVector varBinaryVectorRight = (VarBinaryVector) right;
      int offset = varBinaryVectorLeft.getValueCount();
      for (int i = 0; i < varBinaryVectorRight.getValueCount(); i++) {
        if (varBinaryVectorRight.isNull(i)) {
          varBinaryVectorLeft.setNull(offset + i);
        } else {
          varBinaryVectorLeft.setSafe(offset + i, varBinaryVectorRight.get(i));
        }
      }
      varBinaryVectorLeft.setValueCount(offset + varBinaryVectorRight.getValueCount());
    } else if (left instanceof VarCharVector) {
      VarCharVector varCharVectorLeft = (VarCharVector) left;
      VarCharVector varCharVectorRight = (VarCharVector) right;
      int offset = varCharVectorLeft.getValueCount();
      for (int i = 0; i < varCharVectorRight.getValueCount(); i++) {
        if (varCharVectorRight.isNull(i)) {
          varCharVectorLeft.setNull(offset + i);
        } else {
          varCharVectorLeft.setSafe(offset + i, varCharVectorRight.get(i));
        }
      }
      varCharVectorLeft.setValueCount(offset + varCharVectorRight.getValueCount());
    } else {
      throw new SFException(
          ErrorCode.INTERNAL_ERROR, "Failed to merge vector due to unknown vector type");
    }
  }

  private void sortFirstResultChunk(List<ArrowVectorConverter> converters)
      throws SnowflakeSQLException {
    try {
      List<ValueVector> firstResultChunk = this.batchOfVectors.get(0);
      ArrowResultChunkIndexSorter sorter =
          new ArrowResultChunkIndexSorter(firstResultChunk, converters);
      firstResultChunkSortedIndices = sorter.sort();
    } catch (SFException ex) {
      throw new SnowflakeSQLException(
          ex,
          SqlState.INTERNAL_ERROR,
          ErrorCode.INTERNAL_ERROR.getMessageCode(),
          "Failed to sort first result chunk: " + ex.getLocalizedMessage());
    }
  }

  private boolean sortFirstResultChunkEnabled() {
    return enableSortFirstResultChunk;
  }

  /**
   * Empty arrow result chunk implementation. Used when rowset from server is null or empty or in
   * testing
   */
  private static class EmptyArrowResultChunk extends ArrowResultChunk {
    EmptyArrowResultChunk() {
      super("", 0, 0, 0, null, null);
    }

    @Override
    public final long computeNeededChunkMemory() {
      return 0;
    }

    @Override
    public final void freeData() {
      // do nothing
    }
  }
}
