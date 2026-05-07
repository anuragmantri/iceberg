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
package org.apache.iceberg.spark.extensions;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.source.SparkColumnUpdateReader;
import org.apache.iceberg.spark.source.SparseColumnUpdateJoinReader;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(org.apache.iceberg.ParameterizedTestExtension.class)
public class TestSparseColumnUpdateRead extends ExtensionsTestBase {

  @AfterEach
  public void dropTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  private DataFile writeUpdateFile(
      Table table, Schema updateSchema, List<Record> records, DataFile baseFile)
      throws IOException {
    String filename = "update-" + java.util.UUID.randomUUID() + ".parquet";
    OutputFile outputFile =
        table.io().newOutputFile(table.locationProvider().newDataLocation(filename));

    try (FileAppender<Record> appender =
        Parquet.write(outputFile)
            .schema(updateSchema)
            .createWriterFunc(GenericParquetWriter::create)
            .overwrite()
            .build()) {
      appender.addAll(records);
    }

    // Use the base file's spec to match spec IDs
    return DataFiles.builder(table.specs().get(baseFile.specId()))
        .withPath(outputFile.location())
        .withFileSizeInBytes(outputFile.toInputFile().getLength())
        .withRecordCount(records.size())
        .withFormat(FileFormat.PARQUET)
        .withPartition(baseFile.partition())
        .build();
  }

  private DataFile getOnlyBaseFile(Table table) throws IOException {
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      return tasks.iterator().next().file();
    }
  }

  @TestTemplate
  public void testSparseUpdateSingleColumn() throws IOException, NoSuchTableException {
    // 1. Create a V4 table and insert data
    sql(
        "CREATE TABLE %s (id INT, name STRING, salary LONG) USING iceberg "
            + "TBLPROPERTIES ('format-version'='4')",
        tableName);
    // Use a single-partition insert to ensure one base file
    spark
        .sql(
            String.format(
                "SELECT * FROM VALUES (1, 'Alice', 100L), (2, 'Bob', 200L), (3, 'Charlie', 300L), "
                    + "(4, 'Dave', 400L), (5, 'Eve', 500L) AS t(id, name, salary)"))
        .coalesce(1)
        .writeTo(tableName)
        .append();

    Table table = validationCatalog.loadTable(tableIdent);
    DataFile baseFile = getOnlyBaseFile(table);

    // 2. Write a sparse update file: update salary for rows at positions 1 and 3 (Bob and Dave)
    Schema updateSchema =
        new Schema(
            SparseColumnUpdateJoinReader.STORED_POS_FIELD, // _pos as regular data column
            Types.NestedField.required(3, "salary", Types.LongType.get()));

    GenericRecord rec = GenericRecord.create(updateSchema);
    List<Record> updateRecords =
        Lists.newArrayList(
            rec.copy("_pos", 1L, "salary", 999L), // Bob: 200 -> 999
            rec.copy("_pos", 3L, "salary", 888L)); // Dave: 400 -> 888

    DataFile updateDataFile = writeUpdateFile(table, updateSchema, updateRecords, baseFile);

    // 3. Commit the column update
    table
        .newColumnUpdate()
        .withFieldIds(List.of(3)) // field ID 3 = salary
        .addColumnUpdate(baseFile, updateDataFile)
        .commit();

    table.refresh();
    assertThat(table.snapshots()).hasSize(2);

    // Verify column update metadata is persisted
    DataFile dataFileWithUpdates;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      dataFileWithUpdates = tasks.iterator().next().file();
    }
    assertThat(dataFileWithUpdates.columnUpdateDetails())
        .as("Column update details should be persisted in manifest")
        .isNotNull()
        .isNotEmpty();

    // 4. Read back via Spark and verify
    List<Object[]> result = sql("SELECT id, name, salary FROM %s ORDER BY id", tableName);

    assertThat(result).hasSize(5);
    assertThat(result.get(0)[2]).isEqualTo(100L); // Alice: unchanged
    assertThat(result.get(1)[2]).isEqualTo(999L); // Bob: updated
    assertThat(result.get(2)[2]).isEqualTo(300L); // Charlie: unchanged
    assertThat(result.get(3)[2]).isEqualTo(888L); // Dave: updated
    assertThat(result.get(4)[2]).isEqualTo(500L); // Eve: unchanged
  }

  @TestTemplate
  public void testSparseUpdateAllRows() throws IOException, NoSuchTableException {
    sql(
        "CREATE TABLE %s (id INT, value STRING) USING iceberg "
            + "TBLPROPERTIES ('format-version'='4')",
        tableName);
    spark
        .sql("SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, 'c') AS t(id, value)")
        .coalesce(1)
        .writeTo(tableName)
        .append();

    Table table = validationCatalog.loadTable(tableIdent);
    DataFile baseFile = getOnlyBaseFile(table);

    Schema updateSchema =
        new Schema(
            SparseColumnUpdateJoinReader.STORED_POS_FIELD,
            Types.NestedField.optional(2, "value", Types.StringType.get()));

    GenericRecord rec = GenericRecord.create(updateSchema);
    List<Record> updateRecords =
        Lists.newArrayList(
            rec.copy("_pos", 0L, "value", "x"),
            rec.copy("_pos", 1L, "value", "y"),
            rec.copy("_pos", 2L, "value", "z"));

    DataFile updateDataFile = writeUpdateFile(table, updateSchema, updateRecords, baseFile);

    table
        .newColumnUpdate()
        .withFieldIds(List.of(2))
        .addColumnUpdate(baseFile, updateDataFile)
        .commit();

    List<Object[]> result = sql("SELECT id, value FROM %s ORDER BY id", tableName);
    assertThat(result).hasSize(3);
    assertThat(result.get(0)[1]).isEqualTo("x");
    assertThat(result.get(1)[1]).isEqualTo("y");
    assertThat(result.get(2)[1]).isEqualTo("z");
  }

  @TestTemplate
  public void testSparseUpdateNoRows() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id INT, value STRING) USING iceberg "
            + "TBLPROPERTIES ('format-version'='4')",
        tableName);
    spark
        .sql("SELECT * FROM VALUES (1, 'a'), (2, 'b') AS t(id, value)")
        .coalesce(1)
        .writeTo(tableName)
        .append();

    // Read without any column updates — should work normally
    List<Object[]> result = sql("SELECT id, value FROM %s ORDER BY id", tableName);
    assertThat(result).hasSize(2);
    assertThat(result.get(0)[1]).isEqualTo("a");
    assertThat(result.get(1)[1]).isEqualTo("b");
  }

  @TestTemplate
  public void testSparseUpdateWithDataFrameJoin() throws IOException, NoSuchTableException {
    // Create table and insert data
    sql(
        "CREATE TABLE %s (id INT, name STRING, salary LONG) USING iceberg "
            + "TBLPROPERTIES ('format-version'='4')",
        tableName);
    spark
        .sql(
            "SELECT * FROM VALUES (1, 'Alice', 100L), (2, 'Bob', 200L), (3, 'Charlie', 300L), "
                + "(4, 'Dave', 400L), (5, 'Eve', 500L) AS t(id, name, salary)")
        .coalesce(1)
        .writeTo(tableName)
        .append();

    Table table = validationCatalog.loadTable(tableIdent);
    DataFile baseFile = getOnlyBaseFile(table);

    // Write sparse update file: salary for positions 1 (Bob) and 3 (Dave)
    Schema updateSchema =
        new Schema(
            SparseColumnUpdateJoinReader.STORED_POS_FIELD,
            Types.NestedField.required(3, "salary", Types.LongType.get()));

    GenericRecord rec = GenericRecord.create(updateSchema);
    List<Record> updateRecords =
        Lists.newArrayList(
            rec.copy("_pos", 1L, "salary", 999L), rec.copy("_pos", 3L, "salary", 888L));

    DataFile updateDataFile = writeUpdateFile(table, updateSchema, updateRecords, baseFile);
    String updateFilePath = updateDataFile.location();

    // Commit the column update
    table
        .newColumnUpdate()
        .withFieldIds(List.of(3))
        .addColumnUpdate(baseFile, updateDataFile)
        .commit();

    // Read using Spark DataFrame join (Strategy 1)
    Dataset<Row> result =
        SparkColumnUpdateReader.readWithJoin(spark, tableName, updateFilePath, List.of("salary"));

    List<Row> rows = result.orderBy("id").collectAsList();

    assertThat(rows).hasSize(5);
    assertThat(rows.get(0).getLong(rows.get(0).fieldIndex("salary"))).isEqualTo(100L); // Alice
    assertThat(rows.get(1).getLong(rows.get(1).fieldIndex("salary"))).isEqualTo(999L); // Bob
    assertThat(rows.get(2).getLong(rows.get(2).fieldIndex("salary"))).isEqualTo(300L); // Charlie
    assertThat(rows.get(3).getLong(rows.get(3).fieldIndex("salary"))).isEqualTo(888L); // Dave
    assertThat(rows.get(4).getLong(rows.get(4).fieldIndex("salary"))).isEqualTo(500L); // Eve
  }

  @TestTemplate
  public void testOverlappingFieldIdsDifferentRows() throws IOException, NoSuchTableException {
    // Scenario 3: Two updates share col_2 (field_id=3) but target different rows.
    // Update 1: col_2=val_b, col_1=val_a for positions 0-1
    // Update 2: col_2=val_c, col_3=val_d for positions 2-4
    // After merge: col_2 should have values from BOTH updates.
    sql(
        "CREATE TABLE %s (id INT, name STRING, salary LONG, bonus LONG) USING iceberg "
            + "TBLPROPERTIES ('format-version'='4')",
        tableName);
    spark
        .sql(
            "SELECT * FROM VALUES "
                + "(1, 'Alice', 100L, 10L), (2, 'Bob', 200L, 20L), "
                + "(3, 'Charlie', 300L, 30L), (4, 'Dave', 400L, 40L), "
                + "(5, 'Eve', 500L, 50L) AS t(id, name, salary, bonus)")
        .coalesce(1)
        .writeTo(tableName)
        .append();

    Table table = validationCatalog.loadTable(tableIdent);
    DataFile baseFile = getOnlyBaseFile(table);

    // Update 1: salary (field 3) + name (field 2) for positions 0 and 1
    Schema update1Schema =
        new Schema(
            SparseColumnUpdateJoinReader.STORED_POS_FIELD,
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "salary", Types.LongType.get()));

    GenericRecord rec1 = GenericRecord.create(update1Schema);
    List<Record> update1Records =
        Lists.newArrayList(
            rec1.copy("_pos", 0L, "name", "ALICE", "salary", 111L),
            rec1.copy("_pos", 1L, "name", "BOB", "salary", 222L));

    DataFile update1File = writeUpdateFile(table, update1Schema, update1Records, baseFile);
    table
        .newColumnUpdate()
        .withFieldIds(List.of(2, 3))
        .addColumnUpdate(baseFile, update1File)
        .commit();
    table.refresh();

    // Update 2: salary (field 3) + bonus (field 4) for positions 2, 3, 4
    // salary overlaps with update 1, but targets DIFFERENT rows
    DataFile baseFileAfterUpdate1 = getOnlyBaseFile(table);
    Schema update2Schema =
        new Schema(
            SparseColumnUpdateJoinReader.STORED_POS_FIELD,
            Types.NestedField.required(3, "salary", Types.LongType.get()),
            Types.NestedField.required(4, "bonus", Types.LongType.get()));

    GenericRecord rec2 = GenericRecord.create(update2Schema);
    List<Record> update2Records =
        Lists.newArrayList(
            rec2.copy("_pos", 2L, "salary", 333L, "bonus", 99L),
            rec2.copy("_pos", 3L, "salary", 444L, "bonus", 88L),
            rec2.copy("_pos", 4L, "salary", 555L, "bonus", 77L));

    DataFile update2File =
        writeUpdateFile(table, update2Schema, update2Records, baseFileAfterUpdate1);
    table
        .newColumnUpdate()
        .withFieldIds(List.of(3, 4))
        .addColumnUpdate(baseFileAfterUpdate1, update2File)
        .commit();
    table.refresh();

    // Verify: salary should have values from BOTH updates (merged)
    List<Object[]> result = sql("SELECT id, name, salary, bonus FROM %s ORDER BY id", tableName);

    assertThat(result).hasSize(5);
    // Row 0 (Alice): name=ALICE (update1), salary=111 (update1 merged into col), bonus=10 (base)
    assertThat(result.get(0)[1]).isEqualTo("ALICE");
    assertThat(result.get(0)[2]).isEqualTo(111L);
    assertThat(result.get(0)[3]).isEqualTo(10L);
    // Row 1 (Bob): name=BOB (update1), salary=222 (update1 merged), bonus=20 (base)
    assertThat(result.get(1)[1]).isEqualTo("BOB");
    assertThat(result.get(1)[2]).isEqualTo(222L);
    assertThat(result.get(1)[3]).isEqualTo(20L);
    // Row 2 (Charlie): name=Charlie (base), salary=333 (update2), bonus=99 (update2)
    assertThat(result.get(2)[1]).isEqualTo("Charlie");
    assertThat(result.get(2)[2]).isEqualTo(333L);
    assertThat(result.get(2)[3]).isEqualTo(99L);
    // Row 3 (Dave): name=Dave (base), salary=444 (update2), bonus=88 (update2)
    assertThat(result.get(3)[2]).isEqualTo(444L);
    assertThat(result.get(3)[3]).isEqualTo(88L);
    // Row 4 (Eve): name=Eve (base), salary=555 (update2), bonus=77 (update2)
    assertThat(result.get(4)[2]).isEqualTo(555L);
    assertThat(result.get(4)[3]).isEqualTo(77L);
  }
}
