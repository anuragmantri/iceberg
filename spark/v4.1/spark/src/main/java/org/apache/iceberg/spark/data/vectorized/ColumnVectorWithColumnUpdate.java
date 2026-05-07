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
package org.apache.iceberg.spark.data.vectorized;

import java.util.Arrays;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A column vector that overlays sparse column update values onto a base vector.
 *
 * <p>For each row access, checks an {@code overrideMapping} array to determine whether to read from
 * the base vector (original data) or the update vector (new values from a column update file). This
 * enables efficient vectorized stitching of column updates without copying the entire base vector.
 *
 * <p>The mapping is built from the {@code _pos} column in the update file: for rows that were
 * updated, the mapping points to the corresponding index in the update vector. For all other rows,
 * the mapping contains {@code -1}, indicating the base vector should be used.
 */
public class ColumnVectorWithColumnUpdate extends ColumnVector {
  private static final int NO_OVERRIDE = -1;

  private final ColumnVector baseVector;
  private final ColumnVector updateVector;
  private final int[] overrideMapping;
  private volatile ColumnVectorWithColumnUpdate[] children = null;

  /**
   * Creates a column vector that overlays update values onto a base vector.
   *
   * @param baseVector the original column vector from the base data file
   * @param updateVector the column vector from the column update file
   * @param positions the sorted _pos values from the update file (absolute file positions)
   * @param posLo the start index (inclusive) in positions[] for this batch's range
   * @param posHi the end index (exclusive) in positions[] for this batch's range
   * @param batchStartPos the starting row position of the current batch in the base file
   * @param batchSize the number of rows in the current batch
   * @param updateIndexOffset offset to subtract from position indices to get local update vector
   *     indices (equals the cumulative row count of prior update batches)
   */
  public ColumnVectorWithColumnUpdate(
      ColumnVector baseVector,
      ColumnVector updateVector,
      long[] positions,
      int posLo,
      int posHi,
      long batchStartPos,
      int batchSize,
      int updateIndexOffset) {
    super(baseVector.dataType());
    this.baseVector = baseVector;
    this.updateVector = updateVector;
    this.overrideMapping =
        buildOverrideMapping(positions, posLo, posHi, batchStartPos, batchSize, updateIndexOffset);
  }

  private static int[] buildOverrideMapping(
      long[] positions,
      int posLo,
      int posHi,
      long batchStartPos,
      int batchSize,
      int updateIndexOffset) {
    int[] mapping = new int[batchSize];
    Arrays.fill(mapping, NO_OVERRIDE);
    for (int i = posLo; i < posHi; i++) {
      int batchRow = (int) (positions[i] - batchStartPos);
      mapping[batchRow] = i - updateIndexOffset;
    }
    return mapping;
  }

  @Override
  public void close() {
    // Do not close either vector — base is owned by the batch reader,
    // update is owned by LoadedColumnUpdate
  }

  @Override
  public boolean hasNull() {
    return baseVector.hasNull() || updateVector.hasNull();
  }

  @Override
  public int numNulls() {
    // Overestimate is safe; exact count would be expensive
    return baseVector.numNulls() + updateVector.numNulls();
  }

  @Override
  public boolean isNullAt(int rowId) {
    int idx = overrideMapping[rowId];
    return idx == NO_OVERRIDE ? baseVector.isNullAt(rowId) : updateVector.isNullAt(idx);
  }

  @Override
  public boolean getBoolean(int rowId) {
    int idx = overrideMapping[rowId];
    return idx == NO_OVERRIDE ? baseVector.getBoolean(rowId) : updateVector.getBoolean(idx);
  }

  @Override
  public byte getByte(int rowId) {
    int idx = overrideMapping[rowId];
    return idx == NO_OVERRIDE ? baseVector.getByte(rowId) : updateVector.getByte(idx);
  }

  @Override
  public short getShort(int rowId) {
    int idx = overrideMapping[rowId];
    return idx == NO_OVERRIDE ? baseVector.getShort(rowId) : updateVector.getShort(idx);
  }

  @Override
  public int getInt(int rowId) {
    int idx = overrideMapping[rowId];
    return idx == NO_OVERRIDE ? baseVector.getInt(rowId) : updateVector.getInt(idx);
  }

  @Override
  public long getLong(int rowId) {
    int idx = overrideMapping[rowId];
    return idx == NO_OVERRIDE ? baseVector.getLong(rowId) : updateVector.getLong(idx);
  }

  @Override
  public float getFloat(int rowId) {
    int idx = overrideMapping[rowId];
    return idx == NO_OVERRIDE ? baseVector.getFloat(rowId) : updateVector.getFloat(idx);
  }

  @Override
  public double getDouble(int rowId) {
    int idx = overrideMapping[rowId];
    return idx == NO_OVERRIDE ? baseVector.getDouble(rowId) : updateVector.getDouble(idx);
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    int idx = overrideMapping[rowId];
    return idx == NO_OVERRIDE ? baseVector.getArray(rowId) : updateVector.getArray(idx);
  }

  @Override
  public ColumnarMap getMap(int rowId) {
    int idx = overrideMapping[rowId];
    return idx == NO_OVERRIDE ? baseVector.getMap(rowId) : updateVector.getMap(idx);
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    int idx = overrideMapping[rowId];
    return idx == NO_OVERRIDE
        ? baseVector.getDecimal(rowId, precision, scale)
        : updateVector.getDecimal(idx, precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    int idx = overrideMapping[rowId];
    return idx == NO_OVERRIDE ? baseVector.getUTF8String(rowId) : updateVector.getUTF8String(idx);
  }

  @Override
  public byte[] getBinary(int rowId) {
    int idx = overrideMapping[rowId];
    return idx == NO_OVERRIDE ? baseVector.getBinary(rowId) : updateVector.getBinary(idx);
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    if (children == null) {
      synchronized (this) {
        if (children == null) {
          if (dataType() instanceof StructType) {
            StructType structType = (StructType) dataType();
            this.children = new ColumnVectorWithColumnUpdate[structType.length()];
            for (int index = 0; index < structType.length(); index++) {
              children[index] =
                  new ColumnVectorWithColumnUpdate(
                      baseVector.getChild(index), updateVector.getChild(index), overrideMapping);
            }
          } else {
            throw new UnsupportedOperationException("Unsupported nested type: " + dataType());
          }
        }
      }
    }

    return children[ordinal];
  }

  /**
   * Package-private constructor for wrapping children of struct types, where the override mapping
   * has already been computed by the parent.
   */
  ColumnVectorWithColumnUpdate(
      ColumnVector baseVector, ColumnVector updateVector, int[] overrideMapping) {
    super(baseVector.dataType());
    this.baseVector = baseVector;
    this.updateVector = updateVector;
    this.overrideMapping = overrideMapping;
  }
}
