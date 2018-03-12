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

import org.apache.parquet.schema.Type;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;


public class SimpleWritableColumnVector extends WritableColumnVector {

  public SimpleWritableColumnVector(Type type) {
    super(10, type);
    this.type = type;
  }

  @Override
  public int getDictId(int rowId) {
    return 0;
  }

  @Override
  protected void reserveInternal(int capacity) {
    System.out.println("reserveInternal lol");
  }

  @Override
  public void putNotNull(int rowId) {
    System.out.println("putNotNull lol");
  }

  @Override
  public void putNull(int rowId) {
    System.out.println("putNull lol");
  }

  @Override
  public void putNulls(int rowId, int count) {
    System.out.println("putNulls lol");
  }

  @Override
  public void putNotNulls(int rowId, int count) {
    System.out.println("putNotNulls lol");
  }

  @Override
  public void putBoolean(int rowId, boolean value) {
    System.out.println("lol");
  }

  @Override
  public void putBooleans(int rowId, int count, boolean value) {
    System.out.println("lol");
  }

  @Override
  public void putByte(int rowId, byte value) {
    System.out.println("lol");
  }

  @Override
  public void putBytes(int rowId, int count, byte value) {
    System.out.println("lol");
  }

  @Override
  public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
    System.out.println("lol");
  }

  @Override
  public void putShort(int rowId, short value) {
    System.out.println("lol");
  }

  @Override
  public void putShorts(int rowId, int count, short value) {
    System.out.println("lol");
  }

  @Override
  public void putShorts(int rowId, int count, short[] src, int srcIndex) {
    System.out.println("lol");
  }

  @Override
  public void putShorts(int rowId, int count, byte[] src, int srcIndex) {
    System.out.println("lol");
  }

  @Override
  public void putInt(int rowId, int value) {
    System.out.println("putInt lol");
  }

  @Override
  public void putInts(int rowId, int count, int value) {
    System.out.println("putInts lol");
  }

  @Override
  public void putInts(int rowId, int count, int[] src, int srcIndex) {
    System.out.println("putInts lol");
  }

  @Override
  public void putInts(int rowId, int count, byte[] src, int srcIndex) {
    System.out.println("putInts lol");
  }

  public static int toInt(byte[] bytes, int offset) {
    int ret = 0;
    for (int i=0; i<4 && i+offset<bytes.length; i++) {
      ret <<= 8;
      ret |= (int)bytes[i] & 0xFF;
    }
    return ret;
  }

  @Override
  public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    ByteBuffer bb = ByteBuffer.wrap(src);
    System.out.println(
        "putIntsLittleEndian rowId: "
            + rowId
            + " count: "
            + count
            + " src.size: "
            + src.length
            + " src: "
            + bb.order(ByteOrder.LITTLE_ENDIAN).getInt()
            + " srcIndex: "
            + srcIndex);
  }

  @Override
  public void putLong(int rowId, long value) {
    System.out.println("putLong lol");
  }

  @Override
  public void putLongs(int rowId, int count, long value) {
    System.out.println("putLongs lol");
  }

  @Override
  public void putLongs(int rowId, int count, long[] src, int srcIndex) {
    System.out.println("putLongs lol");
  }

  @Override
  public void putLongs(int rowId, int count, byte[] src, int srcIndex) {
    System.out.println("putLongs lol");
  }

  @Override
  public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    System.out.println("lol");
  }

  @Override
  public void putFloat(int rowId, float value) {
    System.out.println("lol");
  }

  @Override
  public void putFloats(int rowId, int count, float value) {
    System.out.println("lol");
  }

  @Override
  public void putFloats(int rowId, int count, float[] src, int srcIndex) {
    System.out.println("lol");
  }

  @Override
  public void putFloats(int rowId, int count, byte[] src, int srcIndex) {
    System.out.println("lol");
  }

  @Override
  public void putDouble(int rowId, double value) {
    System.out.println("lol");
  }

  @Override
  public void putDoubles(int rowId, int count, double value) {
    System.out.println("lol");
  }

  @Override
  public void putDoubles(int rowId, int count, double[] src, int srcIndex) {
    System.out.println("lol");
  }

  @Override
  public void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
    System.out.println("lol");
  }

  @Override
  public void putArray(int rowId, int offset, int length) {
    System.out.println("lol");
  }

  @Override
  public int putByteArray(int rowId, byte[] value, int offset, int count) {
    return 0;
  }

  @Override
  public int getArrayLength(int rowId) {
    return 0;
  }

  @Override
  public int getArrayOffset(int rowId) {
    return 0;
  }

  @Override
  protected WritableColumnVector reserveNewColumn(int capacity, Type type) {
    return null;
  }

  @Override
  public boolean isNullAt(int rowId) {
    return false;
  }

  @Override
  public boolean getBoolean(int rowId) {
    return false;
  }

  @Override
  public byte getByte(int rowId) {
    return 0;
  }

  @Override
  public short getShort(int rowId) {
    return 0;
  }

  @Override
  public int getInt(int rowId) {
    return 0;
  }

  @Override
  public long getLong(int rowId) {
    return 0;
  }

  @Override
  public float getFloat(int rowId) {
    return 0;
  }

  @Override
  public double getDouble(int rowId) {
    return 0;
  }
}
