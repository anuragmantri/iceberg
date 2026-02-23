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
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.ColumnUpdate;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.DataWriteResult;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.RollingDataWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkWriteConf;
import org.apache.iceberg.spark.SparkWriteRequirements;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DataFileSet;
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
import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.RequiresDistributionAndOrdering;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.connector.write.WriteSummary;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkColumnUpdateWrite extends BaseSparkWrite
    implements Write, RequiresDistributionAndOrdering {
  private static final Logger LOG = LoggerFactory.getLogger(SparkColumnUpdateWrite.class);

  private final JavaSparkContext sparkContext;
  private final SparkCopyOnWriteScan scan;
  private final SparkWriteConf writeConf;
  private final Table table;
  private final String queryId;
  private final FileFormat format;
  private final String applicationId;
  private final boolean wapEnabled;
  private final String wapId;
  private final int outputSpecId;
  private final String branch;
  private final long targetFileSize;
  private final Schema writeSchema;
  private final StructType dsSchema;
  private final Map<String, String> extraSnapshotMetadata;
  private final boolean useFanoutWriter;
  private final SparkWriteRequirements writeRequirements;
  private final Map<String, String> writeProperties;
  private final Set<String> updatedColumns;

  private boolean cleanupOnAbort = false;

  SparkColumnUpdateWrite(
      SparkSession spark,
      SparkCopyOnWriteScan scan,
      Table table,
      SparkWriteConf writeConf,
      LogicalWriteInfo writeInfo,
      String applicationId,
      Schema writeSchema,
      StructType dsSchema,
      SparkWriteRequirements writeRequirements,
      Set<String> updatedColumns) {
    this.sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
    this.scan = scan;
    this.table = table;
    this.writeConf = writeConf;
    this.queryId = writeInfo.queryId();
    this.format = writeConf.dataFileFormat();
    this.applicationId = applicationId;
    this.wapEnabled = writeConf.wapEnabled();
    this.wapId = writeConf.wapId();
    this.branch = writeConf.branch();
    this.targetFileSize = writeConf.targetDataFileSize();
    this.writeSchema = writeSchema;
    this.dsSchema = dsSchema;
    this.extraSnapshotMetadata = writeConf.extraSnapshotMetadata();
    this.useFanoutWriter = writeConf.useFanoutWriter(writeRequirements);
    this.writeRequirements = writeRequirements;
    this.outputSpecId = writeConf.outputSpecId();
    this.writeProperties = writeConf.writeProperties();
    this.updatedColumns = updatedColumns;
  }

  private static final NamedReference FILE_PATH_REF =
      Expressions.column(MetadataColumns.FILE_PATH.name());

  @Override
  public Distribution requiredDistribution() {
    // Cluster by FILE_PATH to ensure all rows for the same base file go to the same partition
    Distribution distribution = Distributions.clustered(new NamedReference[] {FILE_PATH_REF});
    LOG.debug("Requesting {} as write distribution for table {}", distribution, table.name());
    return distribution;
  }

  @Override
  public SortOrder[] requiredOrdering() {
    // Sort by FILE_PATH to ensure rows for the same base file are contiguous
    SortOrder[] ordering =
        new SortOrder[] {Expressions.sort(FILE_PATH_REF, SortDirection.ASCENDING)};
    LOG.debug("Requesting {} as write ordering for table {}", ordering, table.name());
    return ordering;
  }

  private DataFileSet baseFiles() {
    if (scan == null) {
      return DataFileSet.create();
    } else {
      return scan.tasks().stream()
          .map(FileScanTask::file)
          .collect(Collectors.toCollection(DataFileSet::create));
    }
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

  @Override
  public BatchWrite toBatch() {
    return new ColumnUpdateOperation(scan, updatedColumns);
  }

  private class ColumnUpdateOperation implements BatchWrite {
    private final SparkCopyOnWriteScan scan;
    private final Set<String> updatedColumns;

    private ColumnUpdateOperation(SparkCopyOnWriteScan scan, Set<String> updatedColumns) {
      this.scan = scan;
      this.updatedColumns = updatedColumns;
    }

    // TODO gaborkaszab: copy-paste from SparkWrite
    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
      // broadcast the table metadata as the writer factory will be sent to executors
      Broadcast<Table> tableBroadcast =
          sparkContext.broadcast(SerializableTableWithSize.copyOf(table));
      return new ColumnUpdateWriteFactory(
          tableBroadcast,
          queryId,
          format,
          outputSpecId,
          targetFileSize,
          writeSchema,
          dsSchema,
          useFanoutWriter,
          writeProperties);
    }

    @Override
    public boolean useCommitCoordinator() {
      return false;
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
      // TODO gaborkaszab: implement
    }

    @Override
    public String toString() {
      return String.format("SparkColumnUpdateWrite(table=%s, format=%s)", table, format);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
      commit(messages, null);
    }

    @Override
    public void commit(WriterCommitMessage[] messages, WriteSummary summary) {
      List<Integer> updatedFieldIds =
          table.schema().columns().stream()
              .filter(field -> updatedColumns.contains(field.name()))
              .map(Types.NestedField::fieldId)
              .toList();
      // TODO gaborkaszab: fail if not all col names have field IDs in the table

      DataFileSet baseFiles = baseFiles();

      // Create a mapping from base file path to base file for quick lookup
      Map<String, DataFile> baseFilesByPath =
          baseFiles.stream()
              .collect(Collectors.toMap(file -> file.location(), Function.identity()));

      Map<String, DataFile> writtenFiles = writtenFiles(messages);
      // TODO gaborkaszab: check that all data files have a new update file

      // Create mapping from base file to update file
      Map<DataFile, DataFile> baseToUpdateFile = Maps.newHashMap();
      for (Map.Entry<String, DataFile> updateFile : writtenFiles.entrySet()) {
        // TODO gaborkaszab: temp workaround for testing, works only if there is a single base file
        Map.Entry<String, DataFile> anotherUpdateFile =
            Map.entry(baseFiles.iterator().next().location(), updateFile.getValue());
        Preconditions.checkState(
            baseFilesByPath.containsKey(anotherUpdateFile.getKey()),
            "Update file references unknown base file ",
            anotherUpdateFile.getKey());

        baseToUpdateFile.put(
            baseFilesByPath.get(anotherUpdateFile.getKey()), anotherUpdateFile.getValue());
      }

      // TODO gaborkaszab: conflict detection with isolationLevel similar to CoWOperation?

      ColumnUpdate columnUpdate = table.newColumnUpdate().withFieldIds(updatedFieldIds);
      baseToUpdateFile.forEach(columnUpdate::addColumnUpdate);
      columnUpdate.commit();
    }
  }

  public static class TaskCommit implements WriterCommitMessage {
    // Maps base file path to the update file written for that base file
    private final Map<String, DataFile> updateFilesByBasePath;

    TaskCommit(Map<String, DataFile> result) {
      this.updateFilesByBasePath = Maps.newHashMap();
      this.updateFilesByBasePath.putAll(result);
    }

    Map<String, DataFile> updateFilesByBasePath() {
      return updateFilesByBasePath;
    }
  }

  // TODO gaborkaszab: copy-paste from SparkWrite. See what's indeed needed
  private static class ColumnUpdateWriteFactory implements DataWriterFactory {
    private final Broadcast<Table> tableBroadcast;
    private final FileFormat format;
    private final int outputSpecId;
    private final long targetFileSize;
    private final Schema writeSchema;
    private final StructType dsSchema;
    private final boolean useFanoutWriter;
    private final String queryId;
    private final Map<String, String> writeProperties;

    protected ColumnUpdateWriteFactory(
        Broadcast<Table> tableBroadcast,
        String queryId,
        FileFormat format,
        int outputSpecId,
        long targetFileSize,
        Schema writeSchema,
        StructType dsSchema,
        boolean useFanoutWriter,
        Map<String, String> writeProperties) {
      this.tableBroadcast = tableBroadcast;
      this.format = format;
      this.outputSpecId = outputSpecId;
      this.targetFileSize = targetFileSize;
      this.writeSchema = writeSchema;
      this.dsSchema = dsSchema;
      this.useFanoutWriter = useFanoutWriter;
      this.queryId = queryId;
      this.writeProperties = writeProperties;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
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

      /* Function<InternalRow, InternalRow> rowLineageExtractor = new ExtractRowLineage(writeSchema);

      if (spec.isUnpartitioned()) {
        return new SparkWrite.UnpartitionedDataWriter(
            writerFactory, fileFactory, io, spec, targetFileSize, rowLineageExtractor);

      } else {
        return new SparkWrite.PartitionedDataWriter(
            writerFactory,
            fileFactory,
            io,
            spec,
            writeSchema,
            dsSchema,
            targetFileSize,
            useFanoutWriter,
            rowLineageExtractor);
      }*/

      return new ColumnUpdateDataWriter(
          writerFactory, fileFactory, io, spec, targetFileSize, dsSchema);
    }
  }

  // TODO gaborkaszab: the issue with this class is that the FILE_PATH is not part of the schema.
  // Commented the code that get's the file path from the row and opens a new writer if we read a
  // new file path.
  // Works as a very basic writer now.
  private static class ColumnUpdateDataWriter implements DataWriter<InternalRow> {
    private final SparkFileWriterFactory writerFactory;
    private final OutputFileFactory fileFactory;
    private final FileIO io;
    private final PartitionSpec spec;
    private final long targetFileSizeInBytes;
    private final int filePathOrdinal = 1;

    private FileWriter<InternalRow, DataWriteResult> currentWriter;
    private String currentFilePath;
    private boolean closed;

    private final Map<String, DataFile> updateFilesByBasePath;

    private ColumnUpdateDataWriter(
        SparkFileWriterFactory writerFactory,
        OutputFileFactory fileFactory,
        FileIO io,
        PartitionSpec spec,
        long targetFileSize,
        StructType dsSchema) {
      this.writerFactory = writerFactory;
      this.fileFactory = fileFactory;
      this.io = io;
      this.spec = spec;
      this.targetFileSizeInBytes = targetFileSize;
      // this.filePathOrdinal = dsSchema.fieldIndex(MetadataColumns.FILE_PATH.name());
      this.updateFilesByBasePath = Maps.newHashMap();
      this.closed = false;
    }

    @Override
    public void write(InternalRow record) throws IOException {
      String filePath = record.getString(filePathOrdinal);

      if (currentWriter == null /*|| !filePath.equals(currentFilePath)*/) {
        openWriter(filePath);
      }

      currentWriter.write(record);
    }

    // TODO gaborkaszab: consider creating DataWriter here instead of RollingDataWriter
    private void openWriter(String filePath) throws IOException {
      closeCurrentWriter();

      currentWriter =
          new RollingDataWriter<>(
              writerFactory, fileFactory, io, targetFileSizeInBytes, spec, null);
      currentFilePath = filePath;
    }

    private void closeCurrentWriter() throws IOException {
      if (currentWriter != null) {
        currentWriter.close();

        DataWriteResult result = currentWriter.result();
        if (result.dataFiles().isEmpty()) {
          throw new IllegalStateException(
              "Unable to create update file for base file " + currentFilePath);
        }
        if (result.dataFiles().size() > 1) {
          throw new IllegalStateException(
              "Multiple update files created for base file " + currentFilePath);
        }
        currentFilePath = UUID.randomUUID().toString(); // TODO gaborkaszab: remove this
        updateFilesByBasePath.put(currentFilePath, result.dataFiles().getFirst());

        this.currentWriter = null;
        this.currentFilePath = null;
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

      // Cleanup written files on abort
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
}
