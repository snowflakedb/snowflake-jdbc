package net.snowflake.client.core.arrow;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseLargeVariableWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BaseVariableWidthViewVector;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.compare.VectorVisitor;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.UnionVector;

public class EndiannessSwitchVisitor implements VectorVisitor<Void, Void> {
  private static int flipBytes(ArrowBuf buf, int offset, int length) {
    byte[] bytes = new byte[length];
    buf.getBytes(offset, bytes, 0, length);
    for (int i = 0; i < length / 2; i++) {
      byte tmp = bytes[i];
      bytes[i] = bytes[length - i - 1];
      bytes[length - i - 1] = tmp;
    }
    buf.setBytes(offset, bytes, 0, length);
    int result = 0;
    try {
      result = ByteBuffer.wrap(bytes).getInt();
    } catch (Exception ignored) {
    }
    return result;
  }

  @Override
  public Void visit(BaseFixedWidthVector baseFixedWidthVector, Void v) {
    int width = baseFixedWidthVector.getTypeWidth();
    ArrowBuf valueBuffer = baseFixedWidthVector.getDataBuffer();
    for (int i = 0; i < baseFixedWidthVector.getValueCount(); i++) {
      flipBytes(valueBuffer, i * width, width);
    }
    return v;
  }

  @Override
  public Void visit(BaseVariableWidthVector baseVariableWidthVector, Void v) {
    int width = BaseVariableWidthVector.OFFSET_WIDTH;
    ArrowBuf offsetBuffer = baseVariableWidthVector.getOffsetBuffer();
    for (int i = 0; i <= baseVariableWidthVector.getValueCount(); i++) {
      flipBytes(offsetBuffer, i * width, width);
    }
    return v;
  }

  @Override
  public Void visit(BaseLargeVariableWidthVector baseLargeVariableWidthVector, Void v) {
    int width = BaseLargeVariableWidthVector.OFFSET_WIDTH;
    ArrowBuf offsetBuffer = baseLargeVariableWidthVector.getDataBuffer();
    for (int i = 0; i <= baseLargeVariableWidthVector.getValueCount(); i++) {
      flipBytes(offsetBuffer, i * width, width);
    }
    return v;
  }

  @Override
  public Void visit(BaseVariableWidthViewVector baseVariableWidthViewVector, Void v) {
    int lengthWidth = BaseVariableWidthViewVector.LENGTH_WIDTH;
    int prefixWidth = BaseVariableWidthViewVector.PREFIX_WIDTH;
    int bufIndexWidth = BaseVariableWidthViewVector.BUF_INDEX_WIDTH;
    int elementWidth = BaseVariableWidthViewVector.ELEMENT_SIZE;
    int offsetWidth = elementWidth - lengthWidth - prefixWidth - bufIndexWidth;
    int inlineWidth = BaseVariableWidthViewVector.INLINE_SIZE;
    ArrowBuf viewBuffer = baseVariableWidthViewVector.getDataBuffer();
    for (int i = 0; i < baseVariableWidthViewVector.getValueCount(); i++) {
      int length = flipBytes(viewBuffer, i * elementWidth, lengthWidth);
      if (length > inlineWidth) {
        flipBytes(viewBuffer, i * elementWidth + lengthWidth + prefixWidth, bufIndexWidth);
        flipBytes(
            viewBuffer, i * elementWidth + lengthWidth + prefixWidth + bufIndexWidth, offsetWidth);
      }
    }
    return v;
  }

  @Override
  public Void visit(ListVector listVector, Void v) {
    listVector.getDataVector().accept(this, v);

    int offsetWidth = ListVector.OFFSET_WIDTH;
    ArrowBuf offsetBuffer = listVector.getOffsetBuffer();
    for (int i = 0; i <= listVector.getValueCount(); i++) {
      flipBytes(offsetBuffer, i * offsetWidth, offsetWidth);
    }

    return v;
  }

  @Override
  public Void visit(FixedSizeListVector fixedSizeListVector, Void v) {
    return fixedSizeListVector.getDataVector().accept(this, v);
  }

  @Override
  public Void visit(LargeListVector largeListVector, Void v) {
    largeListVector.getDataVector().accept(this, v);

    int offsetWidth = ListVector.OFFSET_WIDTH;
    ArrowBuf offsetBuffer = largeListVector.getOffsetBuffer();
    for (int i = 0; i <= largeListVector.getValueCount(); i++) {
      flipBytes(offsetBuffer, i * offsetWidth, offsetWidth);
    }

    return v;
  }

  @Override
  public Void visit(NonNullableStructVector nonNullableStructVector, Void v) {
    List<FieldVector> children = nonNullableStructVector.getChildrenFromFields();
    for (FieldVector child : children) {
      child.accept(this, v);
    }
    return v;
  }

  @Override
  public Void visit(UnionVector unionVector, Void v) {
    List<FieldVector> children = unionVector.getChildrenFromFields();
    for (FieldVector child : children) {
      child.accept(this, v);
    }

    return v;
  }

  @Override
  public Void visit(DenseUnionVector denseUnionVector, Void v) {
    List<FieldVector> children = denseUnionVector.getChildrenFromFields();
    for (FieldVector child : children) {
      child.accept(this, v);
    }

    int offsetWidth = DenseUnionVector.OFFSET_WIDTH;
    ArrowBuf offsetBuffer = denseUnionVector.getOffsetBuffer();
    for (int i = 0; i < denseUnionVector.getValueCount(); i++) {
      flipBytes(offsetBuffer, i * offsetWidth, offsetWidth);
    }

    return v;
  }

  @Override
  public Void visit(NullVector nullVector, Void v) {
    return v;
  }

  @Override
  public Void visit(ExtensionTypeVector<?> extensionTypeVector, Void v) {
    return v;
  }
}
