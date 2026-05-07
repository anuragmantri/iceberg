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
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * Orchestrates vectorized stitching of column update files onto base data file batches.
 *
 * <p>For a base data file with one or more column updates, this class loads all update files
 * eagerly into memory and then applies them to each batch read from the base file. The stitching
 * replaces column vectors in the batch with wrapped vectors that overlay update values at the
 * correct row positions.
 *
 * <p>Usage pattern:
 *
 * <pre>{@code
 * ColumnUpdateStitcher stitcher = new ColumnUpdateStitcher(schema, updateDetails, io);
 * long pos = 0;
 * for (ColumnarBatch batch : baseBatches) {
 *     batch = stitcher.stitch(batch, pos);
 *     pos += batch.numRows();
 *     // process stitched batch
 * }
 * stitcher.close();
 * }</pre>
 */
class ColumnUpdateStitcher implements Closeable {
  private final List<LoadedColumnUpdate> loadedUpdates;
  private final Map<Integer, Integer> fieldIdToOutputOrdinal;

  /**
   * Creates a stitcher for the given column updates.
   *
   * @param expectedSchema the query's expected output schema
   * @param updateDetails the column update metadata from the manifest entry
   * @param inputFileResolver resolves update file paths to InputFile instances
   */
  ColumnUpdateStitcher(
      Schema expectedSchema,
      List<ContentFile.ColumnUpdateDetails> updateDetails,
      java.util.function.Function<String, InputFile> inputFileResolver) {
    this.fieldIdToOutputOrdinal = buildFieldIdToOrdinalMap(expectedSchema);
    this.loadedUpdates = Lists.newArrayListWithCapacity(updateDetails.size());

    for (ContentFile.ColumnUpdateDetails detail : updateDetails) {
      InputFile updateInputFile = inputFileResolver.apply(detail.filePath());
      loadedUpdates.add(
          LoadedColumnUpdate.load(detail, updateInputFile, expectedSchema, fieldIdToOutputOrdinal));
    }
  }

  /**
   * Stitches all column updates into a base batch.
   *
   * @param baseBatch the batch read from the base data file
   * @param batchStartPos the starting row position of this batch within the base file
   * @return a new ColumnarBatch with updated column vectors (or the same batch if no updates apply)
   */
  ColumnarBatch stitch(ColumnarBatch baseBatch, long batchStartPos) {
    int batchSize = baseBatch.numRows();

    ColumnVector[] vectors = new ColumnVector[baseBatch.numCols()];
    for (int i = 0; i < baseBatch.numCols(); i++) {
      vectors[i] = baseBatch.column(i);
    }

    boolean modified = false;
    for (LoadedColumnUpdate update : loadedUpdates) {
      modified |= update.apply(vectors, batchStartPos, batchSize, fieldIdToOutputOrdinal);
    }

    if (!modified) {
      return baseBatch;
    }

    ColumnarBatch result = new ColumnarBatch(vectors);
    result.setNumRows(batchSize);
    return result;
  }

  private static Map<Integer, Integer> buildFieldIdToOrdinalMap(Schema schema) {
    Map<Integer, Integer> map = Maps.newHashMap();
    List<Types.NestedField> columns = schema.columns();
    for (int i = 0; i < columns.size(); i++) {
      map.put(columns.get(i).fieldId(), i);
    }
    return map;
  }

  @Override
  public void close() throws IOException {
    for (LoadedColumnUpdate update : loadedUpdates) {
      update.close();
    }
    loadedUpdates.clear();
  }
}
