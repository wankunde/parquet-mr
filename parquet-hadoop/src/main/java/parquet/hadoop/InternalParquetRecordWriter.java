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
package parquet.hadoop;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static parquet.Log.DEBUG;
import static parquet.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Map;

import parquet.Log;
import parquet.column.ColumnWriteStore;
import parquet.column.ParquetProperties;
import parquet.column.ParquetProperties.WriterVersion;
import parquet.hadoop.CodecFactory.BytesCompressor;
import parquet.hadoop.api.WriteSupport;
import parquet.io.ColumnIOFactory;
import parquet.io.MessageColumnIO;
import parquet.schema.MessageType;

class InternalParquetRecordWriter<T> {
  private static final Log LOG = Log.getLog(InternalParquetRecordWriter.class);

  private static final int MINIMUM_RECORD_COUNT_FOR_CHECK = 100;
  private static final int MAXIMUM_RECORD_COUNT_FOR_CHECK = 10000;

  private final ParquetFileWriter parquetFileWriter;
  private final WriteSupport<T> writeSupport;
  private final MessageType schema;
  private final Map<String, String> extraMetaData;
  private final long rowGroupSize;
  private long rowGroupSizeThreshold;
  private long nextRowGroupSize;
  private final int pageSize;
  private final BytesCompressor compressor;
  private final boolean validating;
  private final ParquetProperties parquetProperties;

  private long recordCount = 0;
  private long recordCountForNextMemCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;
  private long lastRowGroupEndPos = 0;

  private ColumnWriteStore columnStore;
  private ColumnChunkPageWriteStore pageStore;


  /**
   * @param parquetFileWriter the file to write to
   * @param writeSupport the class to convert incoming records
   * @param schema the schema of the records
   * @param extraMetaData extra meta data to write in the footer of the file
   * @param rowGroupSize the size of a block in the file (this will be approximate)
   * @param compressor the codec used to compress
   */
  public InternalParquetRecordWriter(
      ParquetFileWriter parquetFileWriter,
      WriteSupport<T> writeSupport,
      MessageType schema,
      Map<String, String> extraMetaData,
      long rowGroupSize,
      int pageSize,
      BytesCompressor compressor,
      int dictionaryPageSize,
      boolean enableDictionary,
      boolean validating,
      WriterVersion writerVersion) {
    this.parquetFileWriter = parquetFileWriter;
    this.writeSupport = checkNotNull(writeSupport, "writeSupport");
    this.schema = schema;
    this.extraMetaData = extraMetaData;
    this.rowGroupSize = rowGroupSize;
    this.rowGroupSizeThreshold = rowGroupSize;
    this.nextRowGroupSize = rowGroupSizeThreshold;
    this.pageSize = pageSize;
    this.compressor = compressor;
    this.validating = validating;
    this.parquetProperties = new ParquetProperties(dictionaryPageSize, writerVersion, enableDictionary);
    initStore();
  }

  private void initStore() {
    pageStore = new ColumnChunkPageWriteStore(compressor, schema, pageSize);
    columnStore = parquetProperties.newColumnWriteStore(
        schema,
        pageStore,
        pageSize);
    MessageColumnIO columnIO = new ColumnIOFactory(validating).getColumnIO(schema);
    writeSupport.prepareForWrite(columnIO.getRecordWriter(columnStore));
  }

  public void close() throws IOException, InterruptedException {
    flushRowGroupToStore();
    parquetFileWriter.end(extraMetaData);
  }

  public void write(T value) throws IOException, InterruptedException {
    writeSupport.write(value);
    ++ recordCount;
    checkBlockSizeReached();
  }

  /**
   * @return the total size of data written to the file and buffered in memory
   */
  public long getDataSize() {
    return lastRowGroupEndPos + columnStore.getBufferedSize();
  }

  /**
   * 在 writeSupport 写完当前SimpleGroup(record)后进行内存检查，看内存占用是否满足BlockSize限制。
   *
   * 1. writer 在创建的时候会设置好 blockSize, pageSize, dictionaryPageSize 三个参数
   * 2. 如果当前已经使用的内存大于BlockSize，数据 flush 作为一个Block(RowGroup)
   *
   * 例如：
   * nextRowGroupSize = blockSize = 1024
   *
   * recordCount = 100
   * memSize = 5212
   * recordSize = 5212/ 100 = 521
   *
   * 5212 > (1024 - 2*521) 数据进行Flush
   */
  private void checkBlockSizeReached() throws IOException {
    if (recordCount >= recordCountForNextMemCheck) { // checking the memory size is relatively expensive, so let's not do it for every record.
      long memSize = columnStore.getBufferedSize();
      long recordSize = memSize / recordCount;
      // flush the row group if it is within ~2 records of the limit
      // it is much better to be slightly under size than to be over at all
      if (memSize > (nextRowGroupSize - 2 * recordSize)) {
        LOG.info(format("mem size %,d > %,d: flushing %,d records to disk.", memSize, nextRowGroupSize, recordCount));
        flushRowGroupToStore();
        initStore();
        recordCountForNextMemCheck = min(max(MINIMUM_RECORD_COUNT_FOR_CHECK, recordCount / 2), MAXIMUM_RECORD_COUNT_FOR_CHECK);
        this.lastRowGroupEndPos = parquetFileWriter.getPos();
      } else {
        recordCountForNextMemCheck = min(
            max(MINIMUM_RECORD_COUNT_FOR_CHECK, (recordCount + (long)(nextRowGroupSize / ((float)recordSize))) / 2), // will check halfway
            recordCount + MAXIMUM_RECORD_COUNT_FOR_CHECK // will not look more than max records ahead
            );
        if (DEBUG) LOG.debug(format("Checked mem at %,d will check again at: %,d ", recordCount, recordCountForNextMemCheck));
      }
    }
  }

  /**
   * 所有columns对应的数据写入磁盘，作为一个Block
   *
   * @throws IOException
   */
  private void flushRowGroupToStore()
      throws IOException {
    LOG.info(format("Flushing mem columnStore to file. allocated memory: %,d", columnStore.getAllocatedSize()));
    if (columnStore.getAllocatedSize() > (3 * rowGroupSizeThreshold)) {
      LOG.warn("Too much memory used: " + columnStore.memUsageString());
    }

    if (recordCount > 0) {
      parquetFileWriter.startBlock(recordCount);
      columnStore.flush();
      pageStore.flushToFileWriter(parquetFileWriter);
      recordCount = 0;
      parquetFileWriter.endBlock();
      this.nextRowGroupSize = Math.min(
          parquetFileWriter.getNextRowGroupSize(),
          rowGroupSizeThreshold);
    }

    columnStore = null;
    pageStore = null;
  }

  long getRowGroupSizeThreshold() {
    return rowGroupSizeThreshold;
  }

  void setRowGroupSizeThreshold(long rowGroupSizeThreshold) {
    this.rowGroupSizeThreshold = rowGroupSizeThreshold;
  }

  MessageType getSchema() {
    return this.schema;
  }
}

