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
package org.apache.iceberg.spark.source;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.data.vectorized.ColumnVectorWithColumnUpdate;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * Streams a sparse column update file and applies updates to base batches on the fly.
 *
 * <p>Column update files are sparse: they contain only the updated column values plus a {@code
 * _pos} column indicating which rows in the base file were updated. This class reads only the
 * positions eagerly (into a heap array) but streams the Arrow vector batches on demand, holding at
 * most one batch alive at a time. This keeps direct memory usage bounded regardless of update file
 * size.
 *
 * <p>The streaming approach works because the base file is read sequentially and positions in the
 * update file are sorted — once the base advances past an update batch, that batch is released.
 */
class LoadedColumnUpdate implements Closeable {
  private final long[] positions;
  private final List<Integer> fieldIds;
  private final int totalRows;

  // Streaming state: read batches on demand from the update file
  private final CloseableIterable<ColumnarBatch> reader;
  private final CloseableIterator<ColumnarBatch> iterator;
  private final int[] batchBoundaries; // cumulative row count at each batch start

  // Current active batch (at most one held at a time)
  private ColumnarBatch activeBatch;
  private int activeBatchIndex = -1;

  private LoadedColumnUpdate(
      long[] positions,
      List<Integer> fieldIds,
      int totalRows,
      CloseableIterable<ColumnarBatch> reader,
      CloseableIterator<ColumnarBatch> iterator,
      int[] batchBoundaries) {
    this.positions = positions;
    this.fieldIds = fieldIds;
    this.totalRows = totalRows;
    this.reader = reader;
    this.iterator = iterator;
    this.batchBoundaries = batchBoundaries;
  }

  /**
   * Loads a column update file. Reads positions eagerly (heap) but defers Arrow vector loading.
   *
   * <p>Memory usage: O(update_rows * 8 bytes) for the positions array (heap), plus O(batch_size)
   * for the single active Arrow batch (direct memory). At 2.4M update rows with 10K batch size,
   * this is ~19MB heap + ~80KB direct, vs the previous approach which held ~2.4M rows of Arrow
   * vectors (~200MB+ direct).
   */
  static LoadedColumnUpdate load(
      ContentFile.ColumnUpdateDetails updateDetail,
      InputFile inputFile,
      Schema expectedSchema,
      Map<Integer, Integer> fieldIdToOutputOrdinal) {

    List<Integer> fieldIds = updateDetail.fieldIds();

    List<Types.NestedField> readColumns = Lists.newArrayList();
    readColumns.add(SparseColumnUpdateJoinReader.STORED_POS_FIELD);
    for (int fieldId : fieldIds) {
      if (fieldIdToOutputOrdinal.containsKey(fieldId)) {
        Types.NestedField field = expectedSchema.findField(fieldId);
        if (field != null) {
          readColumns.add(field);
        }
      }
    }

    Schema readSchema = new Schema(readColumns);

    // First pass: read only positions to build the positions array and batch boundaries.
    // We use a separate reader for this pass and close it immediately.
    List<Long> allPositions = Lists.newArrayList();
    List<Integer> batchSizes = Lists.newArrayList();

    try (CloseableIterable<ColumnarBatch> posReader = readBuilder(inputFile, readSchema).build()) {
      for (ColumnarBatch batch : posReader) {
        ColumnVector posVector = batch.column(0);
        for (int i = 0; i < batch.numRows(); i++) {
          allPositions.add(posVector.getLong(i));
        }
        batchSizes.add(batch.numRows());
      }
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to read column update file positions: " + inputFile.location(), e);
    }

    long[] positions = new long[allPositions.size()];
    for (int i = 0; i < allPositions.size(); i++) {
      positions[i] = allPositions.get(i);
    }

    int[] batchBoundaries = new int[batchSizes.size()];
    int cumulative = 0;
    for (int i = 0; i < batchSizes.size(); i++) {
      batchBoundaries[i] = cumulative;
      cumulative += batchSizes.get(i);
    }

    // Second reader: opened for streaming batches on demand during apply()
    CloseableIterable<ColumnarBatch> reader = readBuilder(inputFile, readSchema).build();
    CloseableIterator<ColumnarBatch> iterator = reader.iterator();

    return new LoadedColumnUpdate(
        positions, fieldIds, cumulative, reader, iterator, batchBoundaries);
  }

  private static ReadBuilder<ColumnarBatch, ?> readBuilder(InputFile inputFile, Schema readSchema) {
    return FormatModelRegistry.readBuilder(FileFormat.PARQUET, ColumnarBatch.class, inputFile)
        .project(readSchema);
  }

  /**
   * Applies this column update to a batch's column vectors.
   *
   * <p>Advances the internal iterator to the batch containing the needed positions, wraps the base
   * vectors, and releases any previously held batch.
   */
  boolean apply(
      ColumnVector[] vectors,
      long batchStartPos,
      int batchSize,
      Map<Integer, Integer> fieldIdToOutputOrdinal) {

    int posLo = lowerBound(positions, batchStartPos);
    int posHi = lowerBound(positions, batchStartPos + batchSize);

    if (posLo == posHi) {
      return false;
    }

    // Find which update batch contains posLo
    int neededBatchIdx = findBatchForIndex(posLo);

    // Advance iterator to the needed batch, releasing previous batches
    advanceToBatch(neededBatchIdx);

    if (activeBatch == null) {
      return false;
    }

    int updateIndexOffset = batchBoundaries[activeBatchIndex];

    boolean wrapped = false;
    int updateColOrdinal = 1; // column 0 is _pos; data columns start at 1
    for (int fieldId : fieldIds) {
      Integer outputOrdinal = fieldIdToOutputOrdinal.get(fieldId);
      if (outputOrdinal == null) {
        continue;
      }

      ColumnVector updateVector = activeBatch.column(updateColOrdinal);
      if (updateVector != null) {
        vectors[outputOrdinal] =
            new ColumnVectorWithColumnUpdate(
                vectors[outputOrdinal],
                updateVector,
                positions,
                posLo,
                posHi,
                batchStartPos,
                batchSize,
                updateIndexOffset);
        wrapped = true;
      }
      updateColOrdinal++;
    }

    return wrapped;
  }

  private void advanceToBatch(int targetBatchIdx) {
    while (activeBatchIndex < targetBatchIdx && iterator.hasNext()) {
      // Release previous batch to free direct memory
      if (activeBatch != null) {
        activeBatch.close();
        activeBatch = null;
      }
      activeBatch = iterator.next();
      activeBatchIndex++;
    }
  }

  private int findBatchForIndex(int index) {
    for (int i = batchBoundaries.length - 1; i >= 0; i--) {
      if (batchBoundaries[i] <= index) {
        return i;
      }
    }
    return 0;
  }

  private static int lowerBound(long[] array, long target) {
    int lo = 0;
    int hi = array.length;
    while (lo < hi) {
      int mid = (lo + hi) >>> 1;
      if (array[mid] < target) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    return lo;
  }

  int totalRows() {
    return totalRows;
  }

  long[] positions() {
    return positions;
  }

  @Override
  public void close() throws IOException {
    if (activeBatch != null) {
      activeBatch.close();
      activeBatch = null;
    }
    iterator.close();
    reader.close();
  }
}
