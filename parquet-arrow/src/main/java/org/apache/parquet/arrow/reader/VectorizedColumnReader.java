/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.parquet.arrow.reader;



import java.io.IOException;
import java.util.TimeZone;

import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.*;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;

import static org.apache.parquet.column.ValuesType.REPETITION_LEVEL;

/**
 * Decoder to return values from a single column.
 */
public class VectorizedColumnReader {
  /**
   * Total number of values read.
   */
  private long valuesRead;

  /**
   * value that indicates the end of the current page. That is,
   * if valuesRead == endOfPageValueCount, we are at the end of the page.
   */
  private long endOfPageValueCount;

  /**
   * The dictionary, if this column has dictionary encoding.
   */
  private final Dictionary dictionary =null;

  /**
   * If true, the current page is dictionary encoded.
   */
  private boolean isCurrentPageDictionaryEncoded;

  /**
   * Maximum definition level for this column.
   */
  private final int maxDefLevel;

  /**
   * Repetition/Definition/Value readers.
   */
  private ArrowParquetRecordReaderBase.IntIterator repetitionLevelColumn;
  private ArrowParquetRecordReaderBase.IntIterator definitionLevelColumn;
  private ValuesReader dataColumn;

  // Only set if vectorized decoding is true. This is used instead of the row by row decoding
  // with `definitionLevelColumn`.
  private VectorizedRleValuesReader defColumn;

  /**
   * Total number of values in this column (in this row group).
   */
  private final long totalValueCount;

  /**
   * Total values in the current page.
   */
  private int pageValueCount;

  private final PageReader pageReader;
  private final ColumnDescriptor descriptor;
  private final OriginalType originalType;
  // The timezone conversion to apply to int96 timestamps. Null if no conversion.
  private final TimeZone convertTz;

    public VectorizedColumnReader(
            ColumnDescriptor descriptor,
            OriginalType originalType,
            PageReader pageReader,
            TimeZone convertTz) throws IOException {
        this.descriptor = descriptor;
        this.pageReader = pageReader;
        this.convertTz = convertTz;
        this.originalType = originalType;
        this.maxDefLevel = descriptor.getMaxDefinitionLevel();
        this.totalValueCount = 0;
    }
  /*
  private static final TimeZone UTC = DateTimeUtils.TimeZoneUTC();

  public VectorizedColumnReader(
    ColumnDescriptor descriptor,
    OriginalType originalType,
    PageReader pageReader,
    TimeZone convertTz) throws IOException {
    this.descriptor = descriptor;
    this.pageReader = pageReader;
    this.convertTz = convertTz;
    this.originalType = originalType;
    this.maxDefLevel = descriptor.getMaxDefinitionLevel();

    DictionaryPage dictionaryPage = pageReader.readDictionaryPage();
    if (dictionaryPage != null) {
      try {
        this.dictionary = dictionaryPage.getEncoding().initDictionary(descriptor, dictionaryPage);
        this.isCurrentPageDictionaryEncoded = true;
      } catch (IOException e) {
        throw new IOException("could not decode the dictionary for " + descriptor, e);
      }
    } else {
      this.dictionary = null;
      this.isCurrentPageDictionaryEncoded = false;
    }
    this.totalValueCount = pageReader.getTotalValueCount();
    if (totalValueCount == 0) {
      throw new IOException("totalValueCount == 0");
    }
  }


  private boolean next() throws IOException {
    if (valuesRead >= endOfPageValueCount) {
      if (valuesRead >= totalValueCount) {
        // How do we get here? Throw end of stream exception?
        return false;
      }
      readPage();
    }
    ++valuesRead;
    // TODO: Don't read for flat schemas
    //repetitionLevel = repetitionLevelColumn.nextInt();
    return definitionLevelColumn.nextInt() == maxDefLevel;
  }

  void readBatch(int total, WritableColumnVector column) throws IOException {
    int rowId = 0;
    WritableColumnVector dictionaryIds = null;
    if (dictionary != null) {
      // SPARK-16334: We only maintain a single dictionary per row batch, so that it can be used to
      // decode all previous dictionary encoded pages if we ever encounter a non-dictionary encoded
      // page.
      dictionaryIds = column.reserveDictionaryIds(total);
    }
    while (total > 0) {
      // Compute the number of values we want to read in this page.
      int leftInPage = (int) (endOfPageValueCount - valuesRead);
      if (leftInPage == 0) {
        readPage();
        leftInPage = (int) (endOfPageValueCount - valuesRead);
      }
      int num = Math.min(total, leftInPage);
      if (isCurrentPageDictionaryEncoded) {
        // Read and decode dictionary ids.
        defColumn.readIntegers(
          num, dictionaryIds, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);

        // TIMESTAMP_MILLIS encoded as INT64 can't be lazily decoded as we need to post process
        // the values to add microseconds precision.
        if (column.hasDictionary() || (rowId == 0 &&
          (descriptor.getType() == PrimitiveType.PrimitiveTypeName.INT32 ||
            (descriptor.getType() == PrimitiveType.PrimitiveTypeName.INT64  &&
              originalType != OriginalType.TIMESTAMP_MILLIS) ||
            descriptor.getType() == PrimitiveType.PrimitiveTypeName.FLOAT ||
            descriptor.getType() == PrimitiveType.PrimitiveTypeName.DOUBLE ||
            descriptor.getType() == PrimitiveType.PrimitiveTypeName.BINARY))) {
          // Column vector supports lazy decoding of dictionary values so just set the dictionary.
          // We can't do this if rowId != 0 AND the column doesn't have a dictionary (i.e. some
          // non-dictionary encoded values have already been added).
          column.setDictionary(new ParquetDictionary(dictionary));
        } else {
          decodeDictionaryIds(rowId, num, column, dictionaryIds);
        }
      } else {
        if (column.hasDictionary() && rowId != 0) {
          // This batch already has dictionary encoded values but this new page is not. The batch
          // does not support a mix of dictionary and not so we will decode the dictionary.
          decodeDictionaryIds(0, rowId, column, column.getDictionaryIds());
        }
        column.setDictionary(null);
        switch (descriptor.getType()) {
          case BOOLEAN:
            readBooleanBatch(rowId, num, column);
            break;
          case INT32:
            readIntBatch(rowId, num, column);
            break;
          case INT64:
            readLongBatch(rowId, num, column);
            break;
          case INT96:
            readBinaryBatch(rowId, num, column);
            break;
          case FLOAT:
            readFloatBatch(rowId, num, column);
            break;
          case DOUBLE:
            readDoubleBatch(rowId, num, column);
            break;
          case BINARY:
            readBinaryBatch(rowId, num, column);
            break;
          case FIXED_LEN_BYTE_ARRAY:
            readFixedLenByteArrayBatch(rowId, num, column, descriptor.getTypeLength());
            break;
          default:
            throw new IOException("Unsupported type: " + descriptor.getType());
        }
      }

      valuesRead += num;
      rowId += num;
      total -= num;
    }
  }

  private boolean shouldConvertTimestamps() {
    return convertTz != null && !convertTz.equals(UTC);
  }


  private void decodeDictionaryIds(
    int rowId,
    int num,
    WritableColumnVector column,
    WritableColumnVector dictionaryIds) {
    switch (descriptor.getType()) {
      case INT32:
        if (column.dataType() == DataTypes.IntegerType ||
          DecimalType.is32BitDecimalType(column.dataType())) {
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              column.putInt(i, dictionary.decodeToInt(dictionaryIds.getDictId(i)));
            }
          }
        } else if (column.dataType() == DataTypes.ByteType) {
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              column.putByte(i, (byte) dictionary.decodeToInt(dictionaryIds.getDictId(i)));
            }
          }
        } else if (column.dataType() == DataTypes.ShortType) {
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              column.putShort(i, (short) dictionary.decodeToInt(dictionaryIds.getDictId(i)));
            }
          }
        } else {
          throw new UnsupportedOperationException("Unimplemented type: " + column.dataType());
        }
        break;

      case INT64:
        if (column.dataType() == DataTypes.LongType ||
          DecimalType.is64BitDecimalType(column.dataType()) ||
          originalType == OriginalType.TIMESTAMP_MICROS) {
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              column.putLong(i, dictionary.decodeToLong(dictionaryIds.getDictId(i)));
            }
          }
        } else if (originalType == OriginalType.TIMESTAMP_MILLIS) {
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              column.putLong(i,
                DateTimeUtils.fromMillis(dictionary.decodeToLong(dictionaryIds.getDictId(i))));
            }
          }
        } else {
          throw new UnsupportedOperationException("Unimplemented type: " + column.dataType());
        }
        break;

      case FLOAT:
        for (int i = rowId; i < rowId + num; ++i) {
          if (!column.isNullAt(i)) {
            column.putFloat(i, dictionary.decodeToFloat(dictionaryIds.getDictId(i)));
          }
        }
        break;

      case DOUBLE:
        for (int i = rowId; i < rowId + num; ++i) {
          if (!column.isNullAt(i)) {
            column.putDouble(i, dictionary.decodeToDouble(dictionaryIds.getDictId(i)));
          }
        }
        break;
      case INT96:
        if (column.dataType() == DataTypes.TimestampType) {
          if (!shouldConvertTimestamps()) {
            for (int i = rowId; i < rowId + num; ++i) {
              if (!column.isNullAt(i)) {
                Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(i));
                column.putLong(i, ParquetRowConverter.binaryToSQLTimestamp(v));
              }
            }
          } else {
            for (int i = rowId; i < rowId + num; ++i) {
              if (!column.isNullAt(i)) {
                Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(i));
                long rawTime = ParquetRowConverter.binaryToSQLTimestamp(v);
                long adjTime = DateTimeUtils.convertTz(rawTime, convertTz, UTC);
                column.putLong(i, adjTime);
              }
            }
          }
        } else {
          throw new UnsupportedOperationException();
        }
        break;
      case BINARY:
        // TODO: this is incredibly inefficient as it blows up the dictionary right here. We
        // need to do this better. We should probably add the dictionary data to the ColumnVector
        // and reuse it across batches. This should mean adding a ByteArray would just update
        // the length and offset.
        for (int i = rowId; i < rowId + num; ++i) {
          if (!column.isNullAt(i)) {
            Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(i));
            column.putByteArray(i, v.getBytes());
          }
        }
        break;
      case FIXED_LEN_BYTE_ARRAY:
        // DecimalType written in the legacy mode
        if (DecimalType.is32BitDecimalType(column.dataType())) {
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(i));
              column.putInt(i, (int) ParquetRowConverter.binaryToUnscaledLong(v));
            }
          }
        } else if (DecimalType.is64BitDecimalType(column.dataType())) {
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(i));
              column.putLong(i, ParquetRowConverter.binaryToUnscaledLong(v));
            }
          }
        } else if (DecimalType.isByteArrayDecimalType(column.dataType())) {
          for (int i = rowId; i < rowId + num; ++i) {
            if (!column.isNullAt(i)) {
              Binary v = dictionary.decodeToBinary(dictionaryIds.getDictId(i));
              column.putByteArray(i, v.getBytes());
            }
          }
        } else {
          throw new UnsupportedOperationException();
        }
        break;

      default:
        throw new UnsupportedOperationException("Unsupported type: " + descriptor.getType());
    }
  }

  private void readBooleanBatch(int rowId, int num, WritableColumnVector column) {
    assert(column.dataType() == DataTypes.BooleanType);
    defColumn.readBooleans(
      num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
  }

  private void readIntBatch(int rowId, int num, WritableColumnVector column) {
    // This is where we implement support for the valid type conversions.
    // TODO: implement remaining type conversions
    if (column.dataType() == DataTypes.IntegerType || column.dataType() == DataTypes.DateType ||
      DecimalType.is32BitDecimalType(column.dataType())) {
      defColumn.readIntegers(
        num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else if (column.dataType() == DataTypes.ByteType) {
      defColumn.readBytes(
        num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else if (column.dataType() == DataTypes.ShortType) {
      defColumn.readShorts(
        num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else {
      throw new UnsupportedOperationException("Unimplemented type: " + column.dataType());
    }
  }

  private void readLongBatch(int rowId, int num, WritableColumnVector column) {
    // This is where we implement support for the valid type conversions.
    if (column.dataType() == DataTypes.LongType ||
      DecimalType.is64BitDecimalType(column.dataType()) ||
      originalType == OriginalType.TIMESTAMP_MICROS) {
      defColumn.readLongs(
        num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else if (originalType == OriginalType.TIMESTAMP_MILLIS) {
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          column.putLong(rowId + i, DateTimeUtils.fromMillis(dataColumn.readLong()));
        } else {
          column.putNull(rowId + i);
        }
      }
    } else {
      throw new UnsupportedOperationException("Unsupported conversion to: " + column.dataType());
    }
  }

  private void readFloatBatch(int rowId, int num, WritableColumnVector column) {
    // This is where we implement support for the valid type conversions.
    // TODO: support implicit cast to double?
    if (column.dataType() == DataTypes.FloatType) {
      defColumn.readFloats(
        num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else {
      throw new UnsupportedOperationException("Unsupported conversion to: " + column.dataType());
    }
  }

  private void readDoubleBatch(int rowId, int num, WritableColumnVector column) {
    // This is where we implement support for the valid type conversions.
    // TODO: implement remaining type conversions
    if (column.dataType() == DataTypes.DoubleType) {
      defColumn.readDoubles(
        num, column, rowId, maxDefLevel, (VectorizedValuesReader) dataColumn);
    } else {
      throw new UnsupportedOperationException("Unimplemented type: " + column.dataType());
    }
  }

  private void readBinaryBatch(int rowId, int num, WritableColumnVector column) {
    // This is where we implement support for the valid type conversions.
    // TODO: implement remaining type conversions
    VectorizedValuesReader data = (VectorizedValuesReader) dataColumn;
    if (column.dataType() == DataTypes.StringType || column.dataType() == DataTypes.BinaryType
      || DecimalType.isByteArrayDecimalType(column.dataType())) {
      defColumn.readBinarys(num, column, rowId, maxDefLevel, data);
    } else if (column.dataType() == DataTypes.TimestampType) {
      if (!shouldConvertTimestamps()) {
        for (int i = 0; i < num; i++) {
          if (defColumn.readInteger() == maxDefLevel) {
            // Read 12 bytes for INT96
            long rawTime = ParquetRowConverter.binaryToSQLTimestamp(data.readBinary(12));
            column.putLong(rowId + i, rawTime);
          } else {
            column.putNull(rowId + i);
          }
        }
      } else {
        for (int i = 0; i < num; i++) {
          if (defColumn.readInteger() == maxDefLevel) {
            // Read 12 bytes for INT96
            long rawTime = ParquetRowConverter.binaryToSQLTimestamp(data.readBinary(12));
            long adjTime = DateTimeUtils.convertTz(rawTime, convertTz, UTC);
            column.putLong(rowId + i, adjTime);
          } else {
            column.putNull(rowId + i);
          }
        }
      }
    } else {
      throw new UnsupportedOperationException("Unimplemented type: " + column.dataType());
    }
  }

  private void readFixedLenByteArrayBatch(
    int rowId,
    int num,
    WritableColumnVector column,
    int arrayLen) {
    VectorizedValuesReader data = (VectorizedValuesReader) dataColumn;
    // This is where we implement support for the valid type conversions.
    // TODO: implement remaining type conversions
    if (DecimalType.is32BitDecimalType(column.dataType())) {
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          column.putInt(rowId + i,
            (int) ParquetRowConverter.binaryToUnscaledLong(data.readBinary(arrayLen)));
        } else {
          column.putNull(rowId + i);
        }
      }
    } else if (DecimalType.is64BitDecimalType(column.dataType())) {
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          column.putLong(rowId + i,
            ParquetRowConverter.binaryToUnscaledLong(data.readBinary(arrayLen)));
        } else {
          column.putNull(rowId + i);
        }
      }
    } else if (DecimalType.isByteArrayDecimalType(column.dataType())) {
      for (int i = 0; i < num; i++) {
        if (defColumn.readInteger() == maxDefLevel) {
          column.putByteArray(rowId + i, data.readBinary(arrayLen).getBytes());
        } else {
          column.putNull(rowId + i);
        }
      }
    } else {
      throw new UnsupportedOperationException("Unimplemented type: " + column.dataType());
    }
  }

  private void readPage() {
    DataPage page = pageReader.readPage();
    // TODO: Why is this a visitor?
    page.accept(new DataPage.Visitor<Void>() {
      @Override
      public Void visit(DataPageV1 dataPageV1) {
        try {
          readPageV1(dataPageV1);
          return null;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public Void visit(DataPageV2 dataPageV2) {
        try {
          readPageV2(dataPageV2);
          return null;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }

  private void initDataReader(Encoding dataEncoding, byte[] bytes, int offset) throws IOException {
    this.endOfPageValueCount = valuesRead + pageValueCount;
    if (dataEncoding.usesDictionary()) {
      this.dataColumn = null;
      if (dictionary == null) {
        throw new IOException(
          "could not read page in col " + descriptor +
            " as the dictionary was missing for encoding " + dataEncoding);
      }
      @SuppressWarnings("deprecation")
      Encoding plainDict = Encoding.PLAIN_DICTIONARY; // var to allow warning suppression
      if (dataEncoding != plainDict && dataEncoding != Encoding.RLE_DICTIONARY) {
        throw new UnsupportedOperationException("Unsupported encoding: " + dataEncoding);
      }
      this.dataColumn = new VectorizedRleValuesReader();
      this.isCurrentPageDictionaryEncoded = true;
    } else {
      if (dataEncoding != Encoding.PLAIN) {
        throw new UnsupportedOperationException("Unsupported encoding: " + dataEncoding);
      }
      this.dataColumn = new VectorizedPlainValuesReader();
      this.isCurrentPageDictionaryEncoded = false;
    }

    try {
      dataColumn.initFromPage(pageValueCount, bytes, offset);
    } catch (IOException e) {
      throw new IOException("could not read page in col " + descriptor, e);
    }
  }

  private void readPageV1(DataPageV1 page) throws IOException {
    this.pageValueCount = page.getValueCount();
    ValuesReader rlReader = page.getRlEncoding().getValuesReader(descriptor, REPETITION_LEVEL);
    ValuesReader dlReader;

    // Initialize the decoders.
    if (page.getDlEncoding() != Encoding.RLE && descriptor.getMaxDefinitionLevel() != 0) {
      throw new UnsupportedOperationException("Unsupported encoding: " + page.getDlEncoding());
    }
    int bitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxDefinitionLevel());
    this.defColumn = new VectorizedRleValuesReader(bitWidth);
    dlReader = this.defColumn;
    this.repetitionLevelColumn = new ValuesReaderIntIterator(rlReader);
    this.definitionLevelColumn = new ValuesReaderIntIterator(dlReader);
    try {
      byte[] bytes = page.getBytes().toByteArray();
      rlReader.initFromPage(pageValueCount, bytes, 0);
      int next = rlReader.getNextOffset();
      dlReader.initFromPage(pageValueCount, bytes, next);
      next = dlReader.getNextOffset();
      initDataReader(page.getValueEncoding(), bytes, next);
    } catch (IOException e) {
      throw new IOException("could not read page " + page + " in col " + descriptor, e);
    }
  }

  private void readPageV2(DataPageV2 page) throws IOException {
    this.pageValueCount = page.getValueCount();
    this.repetitionLevelColumn = createRLEIterator(descriptor.getMaxRepetitionLevel(),
      page.getRepetitionLevels(), descriptor);

    int bitWidth = BytesUtils.getWidthFromMaxInt(descriptor.getMaxDefinitionLevel());
    this.defColumn = new VectorizedRleValuesReader(bitWidth);
    this.definitionLevelColumn = new ValuesReaderIntIterator(this.defColumn);
    this.defColumn.initFromBuffer(
      this.pageValueCount, page.getDefinitionLevels().toByteArray());
    try {
      initDataReader(page.getDataEncoding(), page.getData().toByteArray(), 0);
    } catch (IOException e) {
      throw new IOException("could not read page " + page + " in col " + descriptor, e);
    }
  }
  */
}
