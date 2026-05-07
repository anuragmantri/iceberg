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
package org.apache.iceberg;

import java.io.IOException;
import java.util.Iterator;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;

/**
 * Merges overlapping columns from two sparse column update files into a single per-column file.
 *
 * <p>When two sequential column updates overlap on a field_id but target different rows, this
 * utility streams both files sorted by {@code _pos}, merge-sorts them (newer wins for duplicate
 * positions), and writes the result. Memory usage is O(1) — only two records are held at a time.
 */
class ColumnUpdateMerger {

  static final int STORED_POS_FIELD_ID = 2147483545;
  static final Types.NestedField STORED_POS_FIELD =
      Types.NestedField.required(STORED_POS_FIELD_ID, "_pos", Types.LongType.get());

  private ColumnUpdateMerger() {}

  /**
   * Merges a single column from two sparse update files into a new single-column file.
   *
   * <p>Both input files must have {@code _pos} sorted in ascending order. The merge produces a
   * sorted output with the union of positions. For duplicate positions, the value from {@code
   * newFile} wins (last writer wins).
   *
   * @param oldFile the previous update file containing the overlapping column
   * @param newFile the new update file containing the overlapping column
   * @param fieldId the field ID of the overlapping column
   * @param tableSchema the table schema (used to resolve the field type)
   * @param outputFile where to write the merged result
   * @param spec the partition spec for building DataFile metadata
   * @param partition the partition data from the base file
   * @return a DataFile representing the merged single-column file
   */
  static DataFile mergeColumn(
      InputFile oldFile,
      InputFile newFile,
      int fieldId,
      Schema tableSchema,
      OutputFile outputFile,
      PartitionSpec spec,
      StructLike partition) {

    Types.NestedField field = tableSchema.findField(fieldId);
    Schema readSchema = new Schema(STORED_POS_FIELD, field);
    Schema writeSchema = new Schema(STORED_POS_FIELD, field);

    try (CloseableIterable<Record> oldReader =
            FormatModelRegistry.<Record, Object>readBuilder(
                    FileFormat.PARQUET, Record.class, oldFile)
                .project(readSchema)
                .build();
        CloseableIterable<Record> newReader =
            FormatModelRegistry.<Record, Object>readBuilder(
                    FileFormat.PARQUET, Record.class, newFile)
                .project(readSchema)
                .build();
        DataWriter<Record> writer =
            FormatModelRegistry.<Record, Object>dataWriteBuilder(
                    FileFormat.PARQUET,
                    Record.class,
                    EncryptedFiles.plainAsEncryptedOutput(outputFile))
                .schema(writeSchema)
                .spec(spec)
                .partition(partition)
                .build()) {

      Iterator<Record> oldIter = oldReader.iterator();
      Iterator<Record> newIter = newReader.iterator();

      Record oldRecord = oldIter.hasNext() ? copyRecord(oldIter.next(), writeSchema) : null;
      Record newRecord = newIter.hasNext() ? copyRecord(newIter.next(), writeSchema) : null;

      while (oldRecord != null || newRecord != null) {
        if (oldRecord == null) {
          writer.write(newRecord);
          newRecord = newIter.hasNext() ? copyRecord(newIter.next(), writeSchema) : null;
        } else if (newRecord == null) {
          writer.write(oldRecord);
          oldRecord = oldIter.hasNext() ? copyRecord(oldIter.next(), writeSchema) : null;
        } else {
          long oldPos = (Long) oldRecord.get(0);
          long newPos = (Long) newRecord.get(0);

          if (oldPos < newPos) {
            writer.write(oldRecord);
            oldRecord = oldIter.hasNext() ? copyRecord(oldIter.next(), writeSchema) : null;
          } else if (oldPos > newPos) {
            writer.write(newRecord);
            newRecord = newIter.hasNext() ? copyRecord(newIter.next(), writeSchema) : null;
          } else {
            writer.write(newRecord);
            oldRecord = oldIter.hasNext() ? copyRecord(oldIter.next(), writeSchema) : null;
            newRecord = newIter.hasNext() ? copyRecord(newIter.next(), writeSchema) : null;
          }
        }
      }

      writer.close();
      return writer.toDataFile();
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to merge column file: %s", outputFile);
    }
  }

  private static Record copyRecord(Record source, Schema schema) {
    GenericRecord copy = GenericRecord.create(schema);
    for (int i = 0; i < schema.columns().size(); i++) {
      copy.set(i, source.get(i));
    }
    return copy;
  }
}
