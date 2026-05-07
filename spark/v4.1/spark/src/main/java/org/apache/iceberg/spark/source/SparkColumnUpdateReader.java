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

import java.util.List;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * Reads an Iceberg table with sparse column updates by using Spark's DataFrame join infrastructure.
 *
 * <p>For files with column updates, constructs a LEFT JOIN between the base file scan and the
 * column update file(s) on {@code _pos}. Spark's join optimizer chooses the best strategy
 * (BroadcastHashJoin for small update files, SortMergeJoin for large ones).
 */
public class SparkColumnUpdateReader {

  private SparkColumnUpdateReader() {}

  /**
   * Reads a table with column updates applied via Spark DataFrame join.
   *
   * <p>Scans only the columns needed: projected columns + _pos for join key. Updated columns are
   * resolved via COALESCE(update.col, base.col) after the LEFT JOIN.
   *
   * @param spark the Spark session
   * @param tableName the qualified table name for Spark SQL
   * @param updateFilePath path to the sparse column update Parquet file
   * @param updatedColumnNames names of the columns that were updated
   * @return a DataFrame with column updates applied via join (includes all table columns)
   */
  public static Dataset<Row> readWithJoin(
      SparkSession spark,
      String tableName,
      String updateFilePath,
      List<String> updatedColumnNames) {
    return readWithJoin(spark, tableName, updateFilePath, updatedColumnNames, null);
  }

  /**
   * Reads a table with column updates, projecting only the specified columns.
   *
   * <p>The base table scan is narrowed to only read: projected columns + _pos. The update file scan
   * reads only _pos + the updated columns that overlap with the projection. This minimizes I/O on
   * both sides of the join.
   *
   * @param spark the Spark session
   * @param tableName the qualified table name for Spark SQL
   * @param updateFilePath path to the sparse column update Parquet file
   * @param updatedColumnNames names of the columns that were updated
   * @param projectedColumns columns to include in the output (null = all columns)
   * @return a DataFrame with column updates applied, projected to the requested columns
   */
  public static Dataset<Row> readWithJoin(
      SparkSession spark,
      String tableName,
      String updateFilePath,
      List<String> updatedColumnNames,
      List<String> projectedColumns) {

    Set<String> updatedSet = Sets.newHashSet(updatedColumnNames);

    // Build the base scan SQL — only scan needed columns + _pos
    String baseScanSql;
    if (projectedColumns == null) {
      baseScanSql = String.format("SELECT *, _pos FROM %s", tableName);
    } else {
      // Scan projected columns + _pos (deduplicated)
      Set<String> scanCols = Sets.newLinkedHashSet(projectedColumns);
      scanCols.add("_pos");
      baseScanSql = String.format("SELECT %s FROM %s", String.join(", ", scanCols), tableName);
    }

    Dataset<Row> baseDf = spark.sql(baseScanSql);

    // Read sparse update file — Spark will only read columns we select
    Dataset<Row> rawUpdateDf = spark.read().parquet(updateFilePath);

    // From the update file, only keep _pos + updated columns that are in the projection
    List<String> updateSelectCols = Lists.newArrayList("_pos");
    for (String updCol : updatedColumnNames) {
      if (projectedColumns == null || projectedColumns.contains(updCol)) {
        updateSelectCols.add(updCol);
      }
    }
    Dataset<Row> updateDf =
        rawUpdateDf.select(updateSelectCols.stream().map(functions::col).toArray(Column[]::new));

    // Rename _pos in update to avoid ambiguity
    updateDf = updateDf.withColumnRenamed("_pos", "_upd_pos");

    // LEFT JOIN on position
    Dataset<Row> joined =
        baseDf.join(updateDf, baseDf.col("_pos").equalTo(updateDf.col("_upd_pos")), "left");

    // Build output projection:
    // - Updated columns: COALESCE(update.col, base.col)
    // - Non-updated columns: base.col
    // - Drop _pos and _upd_pos
    List<String> outputCols = projectedColumns != null ? projectedColumns : tableColumns(baseDf);

    List<Column> selectCols = Lists.newArrayList();
    for (String colName : outputCols) {
      if (colName.equals("_pos") || colName.equals("_upd_pos")) {
        continue;
      }
      if (updatedSet.contains(colName) && updateSelectCols.contains(colName)) {
        selectCols.add(
            functions.coalesce(updateDf.col(colName), baseDf.col(colName)).alias(colName));
      } else {
        selectCols.add(baseDf.col(colName));
      }
    }

    return joined.select(selectCols.toArray(new Column[0]));
  }

  private static List<String> tableColumns(Dataset<Row> df) {
    List<String> cols = Lists.newArrayList();
    for (String col : df.columns()) {
      if (!col.equals("_pos")) {
        cols.add(col);
      }
    }
    return cols;
  }
}
