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

import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Benchmark comparing sparse column updates (via Spark DataFrame join) vs regular MoR.
 *
 * <p>Matches the format of Gabor's full column update measurements but adds row selectivity
 * dimension. Runs only on hadoop catalog.
 *
 * <p>Dimensions: - Columns updated: 1, 5, 10, 20 - Row selectivity: 1%, 10%, 50%, 100%
 */
@ExtendWith(org.apache.iceberg.ParameterizedTestExtension.class)
public class BenchmarkSparseColumnUpdate extends ExtensionsTestBase {

  // Table config
  private static final int NUM_INT_COLS = 80;
  private static final int NUM_STR_COLS = 20;
  private static final int TOTAL_COLS = NUM_INT_COLS + NUM_STR_COLS;
  private static final int NUM_ROWS = 12_000_000;
  private static final int NUM_FILES = 5;
  private static final int ROWS_PER_FILE = NUM_ROWS / NUM_FILES;
  private static final int WARMUP = 0;
  private static final int ITERATIONS = 1;

  // Benchmark dimensions
  private static final int[] UPDATE_COL_COUNTS = {1, 5, 10, 20};
  private static final int[] SELECTIVITY_MODULI = {100, 10, 2, 1}; // 1%, 10%, 50%, 100%

  @AfterEach
  public void dropTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void benchmarkSparseColumnUpdates() throws IOException, NoSuchTableException {
    assumeThat(catalogName).isEqualTo("testhadoop");

    StringBuilder report = new StringBuilder();
    report.append(
        String.format(
            "\n========== SPARSE COLUMN UPDATE BENCHMARK ==========\n"
                + "Table: %d rows, %d files, %d cols (%d int + %d str)\n\n",
            NUM_ROWS, NUM_FILES, TOTAL_COLS, NUM_INT_COLS, NUM_STR_COLS));

    // Header row
    report.append(String.format("%-12s", "Selectivity"));
    for (int numCols : UPDATE_COL_COUNTS) {
      report.append(String.format("  W%-2d cols (sec)  ", numCols));
    }
    report.append("\n");

    // MoR write benchmarks
    report.append("\n--- MoR UPDATE (write) ---\n");
    for (int mod : SELECTIVITY_MODULI) {
      double sel = mod == 1 ? 100.0 : 100.0 / mod;
      report.append(String.format("%-12s", String.format("%.0f%%", sel)));
      for (int numCols : UPDATE_COL_COUNTS) {
        double sec = benchmarkMoRWrite(numCols, mod);
        report.append(String.format("  %-16.3f", sec));
      }
      report.append("\n");
    }

    // Sparse column update write benchmarks
    report.append("\n--- Sparse Column Update (write) ---\n");
    for (int mod : SELECTIVITY_MODULI) {
      double sel = mod == 1 ? 100.0 : 100.0 / mod;
      report.append(String.format("%-12s", String.format("%.0f%%", sel)));
      for (int numCols : UPDATE_COL_COUNTS) {
        double sec = benchmarkSparseWrite(numCols, mod);
        report.append(String.format("  %-16.3f", sec));
      }
      report.append("\n");
    }

    // Sparse column update read benchmarks (full scan via DataFrame join)
    report.append("\n--- Sparse Column Update (read - full scan) ---\n");
    for (int mod : SELECTIVITY_MODULI) {
      double sel = mod == 1 ? 100.0 : 100.0 / mod;
      report.append(String.format("%-12s", String.format("%.0f%%", sel)));
      for (int numCols : UPDATE_COL_COUNTS) {
        double sec = benchmarkSparseRead(numCols, mod, false);
        report.append(String.format("  %-16.3f", sec));
      }
      report.append("\n");
    }

    // Sparse column update read benchmarks (projected scan — only updated cols)
    report.append("\n--- Sparse Column Update (read - projected scan) ---\n");
    for (int mod : SELECTIVITY_MODULI) {
      double sel = mod == 1 ? 100.0 : 100.0 / mod;
      report.append(String.format("%-12s", String.format("%.0f%%", sel)));
      for (int numCols : UPDATE_COL_COUNTS) {
        double sec = benchmarkSparseRead(numCols, mod, true);
        report.append(String.format("  %-16.3f", sec));
      }
      report.append("\n");
    }

    // Sparse column update read benchmarks (vectorized stitcher - full scan)
    report.append("\n--- Sparse Column Update (read - vectorized full scan) ---\n");
    for (int mod : SELECTIVITY_MODULI) {
      double sel = mod == 1 ? 100.0 : 100.0 / mod;
      report.append(String.format("%-12s", String.format("%.0f%%", sel)));
      for (int numCols : UPDATE_COL_COUNTS) {
        double sec = benchmarkVectorizedRead(numCols, mod, false);
        report.append(String.format("  %-16.3f", sec));
      }
      report.append("\n");
    }

    // Sparse column update read benchmarks (vectorized stitcher - projected scan)
    report.append("\n--- Sparse Column Update (read - vectorized proj scan) ---\n");
    for (int mod : SELECTIVITY_MODULI) {
      double sel = mod == 1 ? 100.0 : 100.0 / mod;
      report.append(String.format("%-12s", String.format("%.0f%%", sel)));
      for (int numCols : UPDATE_COL_COUNTS) {
        double sec = benchmarkVectorizedRead(numCols, mod, true);
        report.append(String.format("  %-16.3f", sec));
      }
      report.append("\n");
    }

    // Written bytes comparison
    report.append("\n--- Written bytes ---\n");
    report.append(String.format("%-12s", ""));
    for (int numCols : UPDATE_COL_COUNTS) {
      report.append(String.format("  W%-2d cols (KB)   ", numCols));
    }
    report.append("\n");
    for (int mod : SELECTIVITY_MODULI) {
      double sel = mod == 1 ? 100.0 : 100.0 / mod;
      report.append(String.format("MoR  %-7s", String.format("%.0f%%", sel)));
      for (int numCols : UPDATE_COL_COUNTS) {
        long bytes = measureMoRWrittenBytes(numCols, mod);
        report.append(String.format("  %-16d", bytes / 1024));
      }
      report.append("\n");
      report.append(String.format("CU   %-7s", String.format("%.0f%%", sel)));
      for (int numCols : UPDATE_COL_COUNTS) {
        long bytes = measureSparseWrittenBytes(numCols, mod);
        report.append(String.format("  %-16d", bytes / 1024));
      }
      report.append("\n");
    }

    report.append("\n====================================================\n");
    System.err.println(report);
  }

  // ===== Benchmark methods =====

  private void rollbackToSnapshot(Table table, long snapshotId) {
    sql("CALL %s.system.rollback_to_snapshot('%s', %d)", catalogName, tableIdent, snapshotId);
    table.refresh();
  }

  private double benchmarkMoRWrite(int numCols, int modulus) throws NoSuchTableException {
    createAndPopulateTable("merge-on-read");
    Table table = validationCatalog.loadTable(tableIdent);
    long baselineSnapshotId = table.currentSnapshot().snapshotId();
    String updateSql = buildMoRUpdateSql(numCols, modulus);

    // Warmup
    for (int i = 0; i < WARMUP; i++) {
      sql(updateSql, tableName);
      rollbackToSnapshot(table, baselineSnapshotId);
    }

    // Measure
    long totalMs = 0;
    for (int i = 0; i < ITERATIONS; i++) {
      long start = System.nanoTime();
      sql(updateSql, tableName);
      totalMs += TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
      if (i < ITERATIONS - 1) {
        rollbackToSnapshot(table, baselineSnapshotId);
      }
    }

    sql("DROP TABLE %s", tableName);
    return (totalMs / (double) ITERATIONS) / 1000.0;
  }

  private double benchmarkSparseWrite(int numCols, int modulus)
      throws IOException, NoSuchTableException {
    createAndPopulateTable(null);
    Table table = validationCatalog.loadTable(tableIdent);
    long baselineSnapshotId = table.currentSnapshot().snapshotId();

    // Measure write time for sparse column update
    long totalMs = 0;
    for (int i = 0; i < ITERATIONS; i++) {
      if (i > 0) {
        rollbackToSnapshot(table, baselineSnapshotId);
      }

      table.refresh();
      DataFile baseFile = getFirstBaseFile(table);
      List<String> colNames = updatedColNames(numCols);
      Schema updateSchema = buildUpdateSchema(table, colNames);
      List<Record> updateRecords =
          buildUpdateRecords(updateSchema, colNames, modulus, baseFile.recordCount());

      long start = System.nanoTime();
      writeSparseUpdate(table, baseFile, updateSchema, updateRecords, colNames);
      totalMs += TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    }

    sql("DROP TABLE %s", tableName);
    return (totalMs / (double) ITERATIONS) / 1000.0;
  }

  private double benchmarkSparseRead(int numCols, int modulus, boolean projected)
      throws IOException, NoSuchTableException {
    createAndPopulateTable(null);
    Table table = validationCatalog.loadTable(tableIdent);
    DataFile baseFile = getFirstBaseFile(table);
    List<String> colNames = updatedColNames(numCols);
    Schema updateSchema = buildUpdateSchema(table, colNames);
    List<Record> updateRecords =
        buildUpdateRecords(updateSchema, colNames, modulus, baseFile.recordCount());
    String updateFilePath =
        writeSparseUpdate(table, baseFile, updateSchema, updateRecords, colNames);

    // Warmup
    for (int i = 0; i < WARMUP; i++) {
      if (projected) {
        SparkColumnUpdateReader.readWithJoin(spark, tableName, updateFilePath, colNames, colNames)
            .collect();
      } else {
        SparkColumnUpdateReader.readWithJoin(spark, tableName, updateFilePath, colNames).collect();
      }
    }

    // Measure
    long totalMs = 0;
    for (int i = 0; i < ITERATIONS; i++) {
      long start = System.nanoTime();
      if (projected) {
        SparkColumnUpdateReader.readWithJoin(spark, tableName, updateFilePath, colNames, colNames)
            .collect();
      } else {
        SparkColumnUpdateReader.readWithJoin(spark, tableName, updateFilePath, colNames).collect();
      }
      totalMs += TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    }

    sql("DROP TABLE %s", tableName);
    return (totalMs / (double) ITERATIONS) / 1000.0;
  }

  private double benchmarkVectorizedRead(int numCols, int modulus, boolean projected)
      throws IOException, NoSuchTableException {
    createAndPopulateTable(null);
    Table table = validationCatalog.loadTable(tableIdent);
    DataFile baseFile = getFirstBaseFile(table);
    List<String> colNames = updatedColNames(numCols);
    Schema updateSchema = buildUpdateSchema(table, colNames);
    List<Record> updateRecords =
        buildUpdateRecords(updateSchema, colNames, modulus, baseFile.recordCount());
    writeSparseUpdate(table, baseFile, updateSchema, updateRecords, colNames);

    String selectCols = projected ? String.join(", ", colNames) : "*";

    // Warmup
    for (int i = 0; i < WARMUP; i++) {
      spark.sql(String.format("SELECT %s FROM %s", selectCols, tableName)).collect();
    }

    // Measure — reads go through BatchDataReader with ColumnUpdateStitcher
    long totalMs = 0;
    for (int i = 0; i < ITERATIONS; i++) {
      long start = System.nanoTime();
      spark.sql(String.format("SELECT %s FROM %s", selectCols, tableName)).collect();
      totalMs += TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    }

    sql("DROP TABLE %s", tableName);
    return (totalMs / (double) ITERATIONS) / 1000.0;
  }

  private long measureMoRWrittenBytes(int numCols, int modulus) throws NoSuchTableException {
    createAndPopulateTable("merge-on-read");
    Table table = validationCatalog.loadTable(tableIdent);
    long baseBefore = totalDataFileBytes(table);
    sql(buildMoRUpdateSql(numCols, modulus), tableName);
    table.refresh();
    long result = totalDataFileBytes(table) + totalDeleteFileBytes(table) - baseBefore;
    sql("DROP TABLE %s", tableName);
    return result;
  }

  private long measureSparseWrittenBytes(int numCols, int modulus)
      throws IOException, NoSuchTableException {
    createAndPopulateTable(null);
    Table table = validationCatalog.loadTable(tableIdent);
    DataFile baseFile = getFirstBaseFile(table);
    List<String> colNames = updatedColNames(numCols);
    Schema updateSchema = buildUpdateSchema(table, colNames);
    List<Record> updateRecords =
        buildUpdateRecords(updateSchema, colNames, modulus, baseFile.recordCount());
    OutputFile outputFile = writeUpdateParquetFile(table, updateSchema, updateRecords);
    long result = outputFile.toInputFile().getLength();
    sql("DROP TABLE %s", tableName);
    return result;
  }

  // ===== Helper methods =====

  private void createAndPopulateTable(String updateMode) throws NoSuchTableException {
    StringBuilder colDefs = new StringBuilder("id INT");
    for (int i = 0; i < NUM_INT_COLS; i++) {
      colDefs.append(String.format(", i%d INT", i));
    }
    for (int i = 0; i < NUM_STR_COLS; i++) {
      colDefs.append(String.format(", s%d STRING", i));
    }

    String props = "'format-version'='4'";
    if (updateMode != null) {
      props +=
          String.format(
              ", 'write.update.mode'='%s', 'write.delete.mode'='merge-on-read'", updateMode);
    }

    sql("CREATE TABLE %s (%s) USING iceberg TBLPROPERTIES (%s)", tableName, colDefs, props);

    // Build value columns
    StringBuilder valueCols = new StringBuilder("CAST(id AS INT) AS id");
    for (int i = 0; i < NUM_INT_COLS; i++) {
      valueCols.append(String.format(", CAST(id * %d AS INT) AS i%d", (i + 1), i));
    }
    for (int i = 0; i < NUM_STR_COLS; i++) {
      valueCols.append(String.format(", CONCAT('str_', CAST(id AS STRING), '_%d') AS s%d", i, i));
    }

    // Insert in batches to avoid OOM — each batch creates one file (~2.4M rows)
    for (int batch = 0; batch < NUM_FILES; batch++) {
      int startId = batch * ROWS_PER_FILE + 1;
      int endId = startId + ROWS_PER_FILE;
      spark
          .sql(String.format("SELECT %s FROM range(%d, %d) AS t(id)", valueCols, startId, endId))
          .coalesce(1)
          .writeTo(tableName)
          .append();
    }
  }

  private List<String> updatedColNames(int numCols) {
    // Use columns from the middle of the int range: i40, i41, ...
    return IntStream.range(0, numCols).mapToObj(i -> "i" + (40 + i)).collect(Collectors.toList());
  }

  private String buildMoRSetClause(int numCols) {
    return updatedColNames(numCols).stream()
        .map(col -> col + " = " + col + " + 1")
        .collect(Collectors.joining(", "));
  }

  private String buildMoRUpdateSql(int numCols, int modulus) {
    String set = buildMoRSetClause(numCols);
    if (modulus == 1) {
      return String.format("UPDATE %%s SET %s", set);
    }
    return String.format("UPDATE %%s SET %s WHERE id %%%% %d = 0", set, modulus);
  }

  private Schema buildUpdateSchema(Table table, List<String> colNames) {
    List<Types.NestedField> fields = Lists.newArrayList();
    fields.add(SparseColumnUpdateJoinReader.STORED_POS_FIELD);
    for (String colName : colNames) {
      Types.NestedField field = table.schema().findField(colName);
      fields.add(Types.NestedField.optional(field.fieldId(), field.name(), field.type()));
    }
    return new Schema(fields);
  }

  private List<Record> buildUpdateRecords(
      Schema updateSchema, List<String> colNames, int modulus, long baseFileRowCount) {
    List<Record> records = Lists.newArrayList();
    GenericRecord rec = GenericRecord.create(updateSchema);
    for (long pos = 0; pos < baseFileRowCount; pos++) {
      int id = (int) pos + 1;
      if (modulus == 1 || id % modulus == 0) {
        GenericRecord copy = GenericRecord.create(updateSchema);
        copy.set(0, pos); // _pos
        for (int c = 0; c < colNames.size(); c++) {
          int colIdx = 40 + c;
          copy.set(1 + c, id * (colIdx + 1) + 1); // original + 1
        }
        records.add(copy);
      }
    }
    return records;
  }

  private String writeSparseUpdate(
      Table table,
      DataFile baseFile,
      Schema updateSchema,
      List<Record> updateRecords,
      List<String> colNames)
      throws IOException {
    OutputFile outputFile = writeUpdateParquetFile(table, updateSchema, updateRecords);

    List<Integer> fieldIds =
        colNames.stream()
            .map(name -> table.schema().findField(name).fieldId())
            .collect(Collectors.toList());

    DataFile updateDataFile =
        DataFiles.builder(table.specs().get(baseFile.specId()))
            .withPath(outputFile.location())
            .withFileSizeInBytes(outputFile.toInputFile().getLength())
            .withRecordCount(updateRecords.size())
            .withFormat(FileFormat.PARQUET)
            .withPartition(baseFile.partition())
            .build();

    table
        .newColumnUpdate()
        .withFieldIds(fieldIds)
        .addColumnUpdate(baseFile, updateDataFile)
        .commit();
    table.refresh();
    return updateDataFile.location();
  }

  private OutputFile writeUpdateParquetFile(
      Table table, Schema updateSchema, List<Record> updateRecords) throws IOException {
    String filename = "update-" + java.util.UUID.randomUUID() + ".parquet";
    OutputFile outputFile =
        table.io().newOutputFile(table.locationProvider().newDataLocation(filename));
    try (FileAppender<Record> appender =
        Parquet.write(outputFile)
            .schema(updateSchema)
            .createWriterFunc(GenericParquetWriter::create)
            .overwrite()
            .build()) {
      appender.addAll(updateRecords);
    }
    return outputFile;
  }

  private DataFile getFirstBaseFile(Table table) throws IOException {
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      return tasks.iterator().next().file();
    }
  }

  private long totalDataFileBytes(Table table) {
    long total = 0;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        total += task.file().fileSizeInBytes();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return total;
  }

  private long totalDeleteFileBytes(Table table) {
    long[] total = {0};
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        task.deletes().forEach(df -> total[0] += df.fileSizeInBytes());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return total[0];
  }
}
