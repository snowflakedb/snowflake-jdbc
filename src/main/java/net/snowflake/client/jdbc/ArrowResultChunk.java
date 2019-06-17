/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.jdbc;

import net.snowflake.client.core.DataConversionContext;
import net.snowflake.client.core.arrow.ArrowVectorConverter;
import net.snowflake.client.core.arrow.BigIntToFixedConverter;
import net.snowflake.client.core.arrow.DoubleToRealConverter;
import net.snowflake.client.core.arrow.IntToDateConverter;
import net.snowflake.client.core.arrow.IntToFixedConverter;
import net.snowflake.client.core.arrow.SmallIntToFixedConverter;
import net.snowflake.client.core.arrow.TinyIntToBooleanConverter;
import net.snowflake.client.core.arrow.TinyIntToFixedConverter;
import net.snowflake.client.core.arrow.VarBinaryToBinaryConverter;
import net.snowflake.client.core.arrow.VarBinaryToTextConverter;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.util.TransferPair;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ArrowResultChunk extends SnowflakeResultChunk
{
  /**
   * A 2-D array of arrow ValueVectors, this list represents data in the whole
   * chunk. Since each chunk is divided into record batchs and each record batch
   * is composed of list of column vectors.
   * <p>
   * So the outer list is list of record batches, inner list represents list of
   * columns
   */
  private List<List<ValueVector>> batchOfVectors;

  /**
   * arrow root allocator
   */
  private static RootAllocator rootAllocator = new RootAllocator(Integer.MAX_VALUE);

  public ArrowResultChunk(String url, int rowCount, int colCount,
                          int uncompressedSize)
  {
    super(url, rowCount, colCount, uncompressedSize);
    this.batchOfVectors = new ArrayList<>();
  }

  private void addBatchData(List<ValueVector> batch)
  {
    batchOfVectors.add(batch);
  }

  /**
   * Read an inputStream of arrow data bytes and load them into java vectors
   * of value.
   * Note, there is no copy of data involved once data is loaded into memory.
   * a.k.a ArrowStreamReader originally allocates the memory to hold vectors,
   * but those memory ownership is transfer into ArrowResultChunk class and once
   * ArrowStreamReader is garbage collected, memory will not be cleared up
   *
   * @param is          inputStream which contain arrow data file in bytes
   * @param resultChunk result chunk that holds the resulted arrow vector
   * @throws IOException if failed to read data as arrow file
   */
  public static void readArrowStream(InputStream is,
                                     ArrowResultChunk resultChunk)
  throws IOException
  {
    try (ArrowStreamReader reader = new ArrowStreamReader(is, rootAllocator))
    {
      while (reader.loadNextBatch())
      {
        List<ValueVector> valueVectors = new ArrayList<>();

        VectorSchemaRoot root = reader.getVectorSchemaRoot();

        for (FieldVector f : root.getFieldVectors())
        {
          // transfer will not copy data but transfer ownership of memory
          // from streamReader to resultChunk
          TransferPair t = f.getTransferPair(rootAllocator);
          t.transfer();
          valueVectors.add(t.getTo());
        }

        resultChunk.addBatchData(valueVectors);
      }
    }
  }

  @Override
  public long computeNeededChunkMemory()
  {
    return getUncompressedSize();
  }

  @Override
  public void freeData()
  {
    batchOfVectors.forEach(list -> list.forEach(ValueVector::clear));
  }

  /**
   * Given a list of arrow vectors (all columns in a single record batch),
   * return list of arrow vector converter. Note, converter is built on top of
   * arrow vector, so that arrow data can be converted back to java data
   *
   * @param vectors list of arrow vectors
   * @return list of converters on top of each converters
   */
  private static List<ArrowVectorConverter> initConverters(
      List<ValueVector> vectors,
      DataConversionContext context)
  {
    List<ArrowVectorConverter> converters = new ArrayList<>();
    vectors.forEach(vector ->
                    {
                      // arrow minor type
                      Types.MinorType type = Types.getMinorTypeForArrowType(
                          vector.getField().getType());

                      // each column's metadata
                      Map<String, String> customMeta = vector.getField().getMetadata();

                      switch (SnowflakeType.valueOf(customMeta.get("logicalType")))
                      {
                        case BOOLEAN:
                          converters.add(new TinyIntToBooleanConverter(vector));
                          break;

                        case FIXED:
                          switch (type)
                          {
                            case TINYINT:
                              converters.add(new TinyIntToFixedConverter(vector));
                              break;

                            case SMALLINT:
                              converters.add(new SmallIntToFixedConverter(vector));
                              break;

                            case INT:
                              converters.add(new IntToFixedConverter(vector));
                              break;

                            case BIGINT:
                              converters.add(new BigIntToFixedConverter(vector));
                              break;
                          }
                          break;

                        case REAL:
                          converters.add(new DoubleToRealConverter(vector));
                          break;

                        case TEXT:
                          converters.add(new VarBinaryToTextConverter(vector));
                          break;

                        case DATE:
                          converters.add(new IntToDateConverter(
                              vector, context.getDateFormatter()));
                          break;

                        case BINARY:
                          converters.add(new VarBinaryToBinaryConverter(
                              vector, context.getBinaryFormatter()));
                          break;

                        //TODO add for other data types converter
                      }
                    });

    return converters;
  }


  /**
   * @return an iterator to iterate over current chunk
   */
  public ArrowChunkIterator getIterator(DataConversionContext context)
  {
    return new ArrowChunkIterator(this, context);
  }

  public static ArrowChunkIterator getEmptyChunkIterator()
  {
    return new ArrowChunkIterator(new EmptyArrowResultChunk());
  }

  /**
   * Iterator class used to go through the arrow chunk row by row
   */
  public static class ArrowChunkIterator
  {
    /**
     * chunk that iterator will iterate through
     */
    private ArrowResultChunk resultChunk;

    /**
     * index of record batch that iterator currently points to
     */
    private int currentRecordBatchIndex;

    /**
     * total number of record batch
     */
    private int totalRecordBatch;

    /**
     * index of row inside current record batch that iterator points to
     */
    private int currentRowInRecordBatch;

    /**
     * number of rows inside current record batch
     */
    private int rowCountInCurrentRecordBatch;

    /**
     * list of converters that attached to current record batch
     * Note: this list is updated every time iterator points to a new record
     * batch
     */
    private List<ArrowVectorConverter> currentConverters;

    /**
     * formatters to each data type
     */
    private final DataConversionContext dataConversionContext;

    ArrowChunkIterator(ArrowResultChunk resultChunk,
                       DataConversionContext conversionContext)
    {
      this.resultChunk = resultChunk;
      this.currentRecordBatchIndex = -1;
      this.totalRecordBatch = resultChunk.batchOfVectors.size();
      this.currentRowInRecordBatch = -1;
      this.rowCountInCurrentRecordBatch = 0;
      this.dataConversionContext = conversionContext;
    }

    ArrowChunkIterator(EmptyArrowResultChunk emptyArrowResultChunk)
    {
      this.resultChunk = emptyArrowResultChunk;
      this.currentRecordBatchIndex = 0;
      this.totalRecordBatch = 0;
      this.currentRowInRecordBatch = -1;
      this.rowCountInCurrentRecordBatch = 0;
      this.currentConverters = Collections.emptyList();
      this.dataConversionContext = null;
    }

    /**
     * advance to next row
     */
    public boolean next()
    {
      currentRowInRecordBatch++;
      if (currentRowInRecordBatch < rowCountInCurrentRecordBatch)
      {
        // still in current recordbatch
        return true;
      }
      else
      {
        currentRecordBatchIndex++;
        if (currentRecordBatchIndex < totalRecordBatch)
        {
          this.currentRowInRecordBatch = 0;
          this.rowCountInCurrentRecordBatch =
              resultChunk.batchOfVectors.get(currentRecordBatchIndex)
                  .get(0).getValueCount();
          currentConverters = initConverters(
              resultChunk.batchOfVectors.get(currentRecordBatchIndex),
              dataConversionContext);

          return true;
        }
      }
      return false;
    }

    public boolean isLast()
    {
      return currentRecordBatchIndex + 1 == totalRecordBatch
             && currentRowInRecordBatch + 1 == rowCountInCurrentRecordBatch;
    }

    public boolean isAfterLast()
    {
      return currentRecordBatchIndex >= totalRecordBatch
             && currentRowInRecordBatch >= rowCountInCurrentRecordBatch;
    }

    public ArrowResultChunk getChunk()
    {
      return resultChunk;
    }

    public ArrowVectorConverter getCurrentConverter(int columnIndex)
    {
      return currentConverters.get(columnIndex);
    }

    /**
     * @return index of row in current record batch
     */
    public int getCurrentRowInRecordBatch()
    {
      return currentRowInRecordBatch;
    }
  }

  /**
   * Empty arrow result chunk implementation. Used when rowset from server is
   * null or empty or in testing
   */
  private static class EmptyArrowResultChunk extends ArrowResultChunk
  {
    EmptyArrowResultChunk()
    {
      super("", 0, 0, 0);
    }

    @Override
    public final long computeNeededChunkMemory()
    {
      return 0;
    }

    @Override
    public final void freeData()
    {
      // do nothing
    }

  }
}
