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
import java.util.stream.Collectors;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.spark.SparkWriteConf;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.write.DeltaWrite;
import org.apache.spark.sql.connector.write.DeltaWriteBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.types.StructType;

class SparkColumnUpdateWriteBuilder implements DeltaWriteBuilder {
  private final SparkSession spark;
  private final Table table;
  private final SparkWriteConf writeConf;
  private final LogicalWriteInfo writeInfo;
  private final StructType dsSchema; // narrow schema — only assigned columns

  SparkColumnUpdateWriteBuilder(
      SparkSession spark,
      Table table,
      String branch,
      LogicalWriteInfo info,
      IsolationLevel isolationLevel) {
    this.spark = spark;
    this.table = table;
    this.writeConf = new SparkWriteConf(spark, table, branch, info.options());
    this.writeInfo = info;
    this.dsSchema = info.schema(); // Spark already narrowed this to assigned columns only
  }

  @Override
  public DeltaWrite build() {
    boolean caseSensitive = writeConf.caseSensitive();

    // dsSchema is the narrow schema (only assigned columns) — convert to get Iceberg field IDs
    Schema narrowIcebergSchema = SparkSchemaUtil.convert(table.schema(), dsSchema, caseSensitive);

    List<Integer> updatedFieldIds =
        narrowIcebergSchema.columns().stream()
            .map(Types.NestedField::fieldId)
            .collect(Collectors.toList());

    // Build update file schema: prepend _pos so the reader can map update rows to base file rows
    // Use a regular field ID (not metadata) so Parquet reads actual stored values
    List<Types.NestedField> updateFileColumns =
        Lists.newArrayList(SparseColumnUpdateJoinReader.STORED_POS_FIELD);
    updateFileColumns.addAll(narrowIcebergSchema.columns());
    Schema updateFileSchema = new Schema(updateFileColumns);
    StructType updateFileSparkSchema = SparkSchemaUtil.convert(updateFileSchema);

    SparkUtil.validatePartitionTransforms(table.spec());

    return new SparkColumnUpdateWrite(
        spark,
        table,
        writeConf,
        writeInfo,
        spark.sparkContext().applicationId(),
        updateFileSchema,
        updateFileSparkSchema,
        updatedFieldIds);
  }
}
