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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.ColumnUpdate;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriteResult;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.RollingDataWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkWriteConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.distributions.Distribution;
import org.apache.spark.sql.connector.distributions.Distributions;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.SortDirection;
import org.apache.spark.sql.connector.expressions.SortOrder;
import org.apache.spark.sql.connector.write.DeltaBatchWrite;
import org.apache.spark.sql.connector.write.DeltaWrite;
import org.apache.spark.sql.connector.write.DeltaWriter;
import org.apache.spark.sql.connector.write.DeltaWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.RequiresDistributionAndOrdering;
import org.apache.spark.sql.connector.write.WriteSummary;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SparkColumnUpdateWrite implements DeltaWrite, RequiresDistributionAndOrdering {
  private static final Logger LOG = LoggerFactory.getLogger(SparkColumnUpdateWrite.class);

  private static final NamedReference FILE_PATH_REF =
      Expressions.column(MetadataColumns.FILE_PATH.name());

  private final JavaSparkContext sparkContext;
  private final SparkWriteConf writeConf;
  private final Table table;
  private final String queryId;
  private final FileFormat format;
  private final int outputSpecId;
  private final long targetFileSize;
  private final Schema writeSchema;
  private final StructType dsSchema;
  private final Map<String, String> writeProperties;
  private final List<Integer> updatedFieldIds;

  SparkColumnUpdateWrite(
      SparkSession spark,
      Table table,
      SparkWriteConf writeConf,
      LogicalWriteInfo writeInfo,
      String applicationId,
      Schema writeSchema,
      StructType dsSchema,
      List<Integer> updatedFieldIds) {
    this.sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
    this.table = table;
    this.writeConf = writeConf;
    this.queryId = writeInfo.queryId();
    this.format = writeConf.dataFileFormat();
    this.outputSpecId = writeConf.outputSpecId();
    this.targetFileSize = writeConf.targetDataFileSize();
    this.writeSchema = writeSchema;
    this.dsSchema = dsSchema;
    this.writeProperties = writeConf.writeProperties();
    this.updatedFieldIds = updatedFieldIds;
  }

  @Override
  public Distribution requiredDistribution() {
    Distribution distribution = Distributions.clustered(new NamedReference[] {FILE_PATH_REF});
    LOG.debug("Requesting {} as write distribution for table {}", distribution, table.name());
    return distribution;
  }

  @Override
  public SortOrder[] requiredOrdering() {
    SortOrder[] ordering =
        new SortOrder[] {Expressions.sort(FILE_PATH_REF, SortDirection.ASCENDING)};
    LOG.debug("Requesting {} as write ordering for table {}", ordering, table.name());
    return ordering;
  }

  @Override
  public DeltaBatchWrite toBatch() {
    return new ColumnUpdateDeltaBatchWrite(updatedFieldIds);
  }

  private Map<String, DataFile> baseFilesByPath() {
    Map<String, DataFile> result = Maps.newHashMap();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        result.put(task.file().location(), task.file());
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to scan base files for column update commit", e);
    }
    return result;
  }

  private Map<String, DataFile> writtenFiles(WriterCommitMessage[] messages) {
    Map<String, DataFile> result = Maps.newHashMap();
    for (WriterCommitMessage message : messages) {
      if (message != null) {
        TaskCommit taskCommit = (TaskCommit) message;
        result.putAll(taskCommit.updateFilesByBasePath());
      }
    }
    return result;
  }

  private class ColumnUpdateDeltaBatchWrite implements DeltaBatchWrite {

    private final List<Integer> updatedFieldIds;

    ColumnUpdateDeltaBatchWrite(List<Integer> updatedFieldIds) {
      this.updatedFieldIds = updatedFieldIds;
    }

    @Override
    public DeltaWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
      Broadcast<Table> tableBroadcast =
          sparkContext.broadcast(SerializableTableWithSize.copyOf(table));
      return new ColumnUpdateDeltaWriterFactory(
          tableBroadcast,
          queryId,
          format,
          outputSpecId,
          targetFileSize,
          writeSchema,
          dsSchema,
          writeProperties);
    }

    @Override
    public boolean useCommitCoordinator() {
      return false;
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
      Map<String, DataFile> writtenFiles = writtenFiles(messages);
      for (DataFile file : writtenFiles.values()) {
        table.io().deleteFile(file.location());
      }
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
      commit(messages, null);
    }

    @Override
    public void commit(WriterCommitMessage[] messages, WriteSummary summary) {
      Map<String, DataFile> baseFilesByPath = baseFilesByPath();
      Map<String, DataFile> writtenFiles = writtenFiles(messages);

      Map<DataFile, DataFile> baseToUpdateFile = Maps.newHashMap();
      for (Map.Entry<String, DataFile> entry : writtenFiles.entrySet()) {
        String basePath = entry.getKey();
        DataFile updateFile = entry.getValue();
        Preconditions.checkState(
            baseFilesByPath.containsKey(basePath),
            "Update file references unknown base file: %s",
            basePath);
        baseToUpdateFile.put(baseFilesByPath.get(basePath), updateFile);
      }

      ColumnUpdate columnUpdate = table.newColumnUpdate().withFieldIds(updatedFieldIds);
      baseToUpdateFile.forEach(columnUpdate::addColumnUpdate);
      columnUpdate.commit();
    }
  }

  static class TaskCommit implements WriterCommitMessage {
    private final Map<String, DataFile> updateFilesByBasePath;

    TaskCommit(Map<String, DataFile> result) {
      this.updateFilesByBasePath = Maps.newHashMap(result);
    }

    Map<String, DataFile> updateFilesByBasePath() {
      return updateFilesByBasePath;
    }
  }

  private static class ColumnUpdateDeltaWriterFactory implements DeltaWriterFactory {
    private final Broadcast<Table> tableBroadcast;
    private final String queryId;
    private final FileFormat format;
    private final int outputSpecId;
    private final long targetFileSize;
    private final Schema writeSchema;
    private final StructType dsSchema;
    private final Map<String, String> writeProperties;

    ColumnUpdateDeltaWriterFactory(
        Broadcast<Table> tableBroadcast,
        String queryId,
        FileFormat format,
        int outputSpecId,
        long targetFileSize,
        Schema writeSchema,
        StructType dsSchema,
        Map<String, String> writeProperties) {
      this.tableBroadcast = tableBroadcast;
      this.queryId = queryId;
      this.format = format;
      this.outputSpecId = outputSpecId;
      this.targetFileSize = targetFileSize;
      this.writeSchema = writeSchema;
      this.dsSchema = dsSchema;
      this.writeProperties = writeProperties;
    }

    @Override
    public DeltaWriter<InternalRow> createWriter(int partitionId, long taskId) {
      Table table = tableBroadcast.value();
      PartitionSpec spec = table.specs().get(outputSpecId);
      FileIO io = table.io();

      OutputFileFactory fileFactory =
          OutputFileFactory.builderFor(table, partitionId, taskId)
              .format(format)
              .operationId(queryId)
              .suffix("update")
              .build();

      SparkFileWriterFactory writerFactory =
          SparkFileWriterFactory.builderFor(table)
              .dataFileFormat(format)
              .dataSchema(writeSchema)
              .dataSparkType(dsSchema)
              .writeProperties(writeProperties)
              .build();

      return new ColumnUpdateDeltaWriter(writerFactory, fileFactory, io, spec, targetFileSize);
    }
  }

  private static class ColumnUpdateDeltaWriter implements DeltaWriter<InternalRow> {
    private static final int FILE_PATH_ORDINAL = 0; // ordinal in the id row
    private static final int ROW_POSITION_ORDINAL = 1; // ordinal in the id row

    private final SparkFileWriterFactory writerFactory;
    private final OutputFileFactory fileFactory;
    private final FileIO io;
    private final PartitionSpec spec;
    private final long targetFileSizeInBytes;

    private FileWriter<InternalRow, DataWriteResult> currentWriter = null;
    private String currentFilePath = null;
    private boolean closed = false;

    private final Map<String, DataFile> updateFilesByBasePath = Maps.newHashMap();

    ColumnUpdateDeltaWriter(
        SparkFileWriterFactory writerFactory,
        OutputFileFactory fileFactory,
        FileIO io,
        PartitionSpec spec,
        long targetFileSize) {
      this.writerFactory = writerFactory;
      this.fileFactory = fileFactory;
      this.io = io;
      this.spec = spec;
      this.targetFileSizeInBytes = targetFileSize;
    }

    @Override
    public void update(InternalRow meta, InternalRow id, InternalRow row) throws IOException {
      String filePath = id.getString(FILE_PATH_ORDINAL);
      long pos = id.getLong(ROW_POSITION_ORDINAL);

      if (!filePath.equals(currentFilePath)) {
        // rows are sorted by FILE_PATH so a path change means we've moved to a new base file
        closeCurrentWriter();
        currentFilePath = filePath;
        currentWriter =
            new RollingDataWriter<>(
                writerFactory, fileFactory, io, targetFileSizeInBytes, spec, null);
      }

      // Write _pos as the first column followed by the assigned column values
      currentWriter.write(new ColumnUpdateRow(pos, row));
    }

    @Override
    public void delete(InternalRow meta, InternalRow id) {
      throw new UnsupportedOperationException("Column update does not delete rows");
    }

    @Override
    public void insert(InternalRow row) {
      throw new UnsupportedOperationException("Column update does not insert rows");
    }

    private void closeCurrentWriter() throws IOException {
      if (currentWriter != null) {
        currentWriter.close();
        DataWriteResult result = currentWriter.result();
        Preconditions.checkState(
            !result.dataFiles().isEmpty(),
            "No update file written for base file: %s",
            currentFilePath);
        Preconditions.checkState(
            result.dataFiles().size() == 1,
            "Multiple update files written for base file: %s",
            currentFilePath);
        updateFilesByBasePath.put(currentFilePath, result.dataFiles().get(0));
        currentWriter = null;
        currentFilePath = null;
      }
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
      close();
      return new TaskCommit(updateFilesByBasePath);
    }

    @Override
    public void abort() throws IOException {
      close();
      for (DataFile file : updateFilesByBasePath.values()) {
        io.deleteFile(file.location());
      }
    }

    @Override
    public void close() throws IOException {
      if (!closed) {
        closeCurrentWriter();
        closed = true;
      }
    }
  }

  /**
   * A zero-copy InternalRow that prepends a _pos (long) value before the delegate row's fields.
   *
   * <p>Ordinal 0 returns the row position; ordinals 1..N delegate to the wrapped row at ordinal-1.
   * This follows the same pattern as Spark's JoinedRow.
   */
  static class ColumnUpdateRow extends InternalRow {
    private final long pos;
    private final InternalRow delegate;

    ColumnUpdateRow(long pos, InternalRow delegate) {
      this.pos = pos;
      this.delegate = delegate;
    }

    @Override
    public int numFields() {
      return 1 + delegate.numFields();
    }

    @Override
    public boolean isNullAt(int ordinal) {
      return ordinal == 0 ? false : delegate.isNullAt(ordinal - 1);
    }

    @Override
    public boolean getBoolean(int ordinal) {
      return delegate.getBoolean(ordinal - 1);
    }

    @Override
    public byte getByte(int ordinal) {
      return ordinal == 0 ? (byte) pos : delegate.getByte(ordinal - 1);
    }

    @Override
    public short getShort(int ordinal) {
      return ordinal == 0 ? (short) pos : delegate.getShort(ordinal - 1);
    }

    @Override
    public int getInt(int ordinal) {
      return ordinal == 0 ? (int) pos : delegate.getInt(ordinal - 1);
    }

    @Override
    public long getLong(int ordinal) {
      return ordinal == 0 ? pos : delegate.getLong(ordinal - 1);
    }

    @Override
    public float getFloat(int ordinal) {
      return delegate.getFloat(ordinal - 1);
    }

    @Override
    public double getDouble(int ordinal) {
      return delegate.getDouble(ordinal - 1);
    }

    @Override
    public org.apache.spark.sql.types.Decimal getDecimal(int ordinal, int precision, int scale) {
      return delegate.getDecimal(ordinal - 1, precision, scale);
    }

    @Override
    public org.apache.spark.unsafe.types.UTF8String getUTF8String(int ordinal) {
      return delegate.getUTF8String(ordinal - 1);
    }

    @Override
    public byte[] getBinary(int ordinal) {
      return delegate.getBinary(ordinal - 1);
    }

    @Override
    public InternalRow getStruct(int ordinal, int numFields) {
      return delegate.getStruct(ordinal - 1, numFields);
    }

    @Override
    public org.apache.spark.sql.catalyst.util.ArrayData getArray(int ordinal) {
      return delegate.getArray(ordinal - 1);
    }

    @Override
    public org.apache.spark.sql.catalyst.util.MapData getMap(int ordinal) {
      return delegate.getMap(ordinal - 1);
    }

    @Override
    public org.apache.spark.unsafe.types.VariantVal getVariant(int ordinal) {
      return delegate.getVariant(ordinal - 1);
    }

    @Override
    public org.apache.spark.unsafe.types.CalendarInterval getInterval(int ordinal) {
      return delegate.getInterval(ordinal - 1);
    }

    @Override
    public org.apache.spark.unsafe.types.GeographyVal getGeography(int ordinal) {
      return delegate.getGeography(ordinal - 1);
    }

    @Override
    public org.apache.spark.unsafe.types.GeometryVal getGeometry(int ordinal) {
      return delegate.getGeometry(ordinal - 1);
    }

    @Override
    public Object get(int ordinal, org.apache.spark.sql.types.DataType dataType) {
      return ordinal == 0 ? pos : delegate.get(ordinal - 1, dataType);
    }

    @Override
    public InternalRow copy() {
      return new ColumnUpdateRow(pos, delegate.copy());
    }

    @Override
    public void setNullAt(int ordinal) {
      throw new UnsupportedOperationException("ColumnUpdateRow is read-only");
    }

    @Override
    public void update(int ordinal, Object value) {
      throw new UnsupportedOperationException("ColumnUpdateRow is read-only");
    }
  }
}
