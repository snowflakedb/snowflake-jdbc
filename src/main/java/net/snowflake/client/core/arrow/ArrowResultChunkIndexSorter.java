package net.snowflake.client.core.arrow;

import java.util.List;
import java.util.stream.IntStream;
import net.snowflake.client.core.SFException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;

/**
 * Use quick sort to sort Arrow result chunk The sorted order is represented in the indices vector
 */
public class ArrowResultChunkIndexSorter {
  private List<ValueVector> resultChunk;
  private List<ArrowVectorConverter> converters;
  /** Vector indices to sort. */
  private IntVector indices;

  public ArrowResultChunkIndexSorter(
      List<ValueVector> resultChunk, List<ArrowVectorConverter> converters) {
    this.resultChunk = resultChunk;
    this.converters = converters;
    initIndices();
  }

  /** initialize original indices */
  private void initIndices() {
    BufferAllocator rootAllocator = resultChunk.get(0).getAllocator();
    FieldType fieldType = new FieldType(true, Types.MinorType.INT.getType(), null, null);

    indices = new IntVector("indices", fieldType, rootAllocator);
    IntStream.range(0, resultChunk.get(0).getValueCount()).forEach(i -> indices.setSafe(i, i));
  }

  /**
   * This method is only used when sf-property sort is on
   *
   * @return sorted indices
   * @throws SFException when exception encountered
   */
  public IntVector sort() throws SFException {
    quickSort(0, resultChunk.get(0).getValueCount() - 1);
    return indices;
  }

  private void quickSort(int low, int high) throws SFException {
    if (low < high) {
      int mid = partition(low, high);
      quickSort(low, mid - 1);
      quickSort(mid + 1, high);
    }
  }

  private int partition(int low, int high) throws SFException {
    int pivotIndex = indices.get(low);

    while (low < high) {
      while (low < high && compare(indices.get(high), pivotIndex) >= 0) {
        high -= 1;
      }
      indices.set(low, indices.get(high));

      while (low < high && compare(indices.get(low), pivotIndex) <= 0) {
        low += 1;
      }
      indices.set(high, indices.get(low));
    }

    indices.setSafe(low, pivotIndex);
    return low;
  }

  /**
   * Implement the same compare method as JSON result
   *
   * @throws SFException
   */
  private int compare(int index1, int index2) throws SFException {
    int numCols = converters.size();
    for (int colIdx = 0; colIdx < numCols; colIdx++) {
      if (converters.get(colIdx).isNull(index1) && converters.get(colIdx).isNull(index2)) {
        continue;
      }

      // null is considered bigger than all values
      if (converters.get(colIdx).isNull(index1)) {
        return 1;
      }

      if (converters.get(colIdx).isNull(index2)) {
        return -1;
      }

      int res =
          converters
              .get(colIdx)
              .toString(index1)
              .compareTo(converters.get(colIdx).toString(index2));

      // continue to next column if no difference
      if (res == 0) {
        continue;
      }

      return res;
    }

    // all columns are the same
    return 0;
  }
}
