// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_LOG_WRITER_H_
#define STORAGE_LEVELDB_DB_LOG_WRITER_H_

#include <cstdint>

#include "db/log_format.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb {

class WritableFile;

namespace log {

// WAL 日志写入器：将记录按块格式写入日志文件
class Writer {
 public:
  // 创建写入器，追加数据到目标文件
  // dest 必须初始为空，且在 Writer 使用期间保持有效
  explicit Writer(WritableFile* dest);

  // 创建写入器，追加数据到已有长度的目标文件
  // dest 必须有初始长度 dest_length，且在 Writer 使用期间保持有效
  Writer(WritableFile* dest, uint64_t dest_length);

  Writer(const Writer&) = delete;
  Writer& operator=(const Writer&) = delete;

  ~Writer();

  // 添加一条记录到日志文件
  Status AddRecord(const Slice& slice);

 private:
  // 发射物理记录到文件（处理分片和格式化）
  Status EmitPhysicalRecord(RecordType type, const char* ptr, size_t length);

  WritableFile* dest_;  // 目标文件
  int block_offset_;    // 当前块内偏移量（0-32KB）

  // 预计算的记录类型 CRC32 值，减少计算开销
  // crc32c values for all supported record types.  These are
  // pre-computed to reduce the overhead of computing the crc of the
  // record type stored in the header.
  uint32_t type_crc_[kMaxRecordType + 1];
};

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_WRITER_H_
