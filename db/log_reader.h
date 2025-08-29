// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_LOG_READER_H_
#define STORAGE_LEVELDB_DB_LOG_READER_H_

#include <cstdint>

#include "db/log_format.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb {

class SequentialFile;

namespace log {

// WAL 日志读取器：从日志文件中读取和重组记录
class Reader {
 public:
  // 错误报告接口
  class Reporter {
   public:
    virtual ~Reporter();

    // 检测到数据损坏时调用
    // bytes: 丢弃的字节数，status: 错误详情
    virtual void Corruption(size_t bytes, const Status& status) = 0;
  };

  // 创建日志读取器
  // file: 源文件，必须在 Reader 使用期间保持有效
  // reporter: 错误报告器，可为 null
  // checksum: 是否验证校验和
  // initial_offset: 开始读取的物理位置
  Reader(SequentialFile* file, Reporter* reporter, bool checksum,
         uint64_t initial_offset);

  Reader(const Reader&) = delete;
  Reader& operator=(const Reader&) = delete;

  ~Reader();

  // 读取下一条记录到 record 中
  // 成功返回 true，到达文件末尾返回 false
  // scratch 用作临时存储，record 内容仅在下次调用前有效
  bool ReadRecord(Slice* record, std::string* scratch);

  // 返回最后一条记录的物理偏移量
  // 在首次调用 ReadRecord 前未定义
  uint64_t LastRecordOffset();

 private:
  // 扩展记录类型的特殊值
  // Extend record types with the following special values
  enum {
    kEof = kMaxRecordType + 1,           // 文件结束
    // 发现无效物理记录时返回的值，三种情况：
    // * 记录有无效 CRC（ReadPhysicalRecord 报告丢弃）
    // * 记录长度为 0（不报告丢弃）
    // * 记录在构造函数的 initial_offset 之前（不报告丢弃）
    kBadRecord = kMaxRecordType + 2      // 坏记录
  };

  // 跳过完全在 initial_offset_ 之前的所有块
  // 成功返回 true，处理错误报告
  bool SkipToInitialBlock();

  // 返回记录类型或上述特殊值之一
  // Return type, or one of the preceding special values
  unsigned int ReadPhysicalRecord(Slice* result);

  // 向报告器报告丢弃的字节
  // 调用前必须更新 buffer_ 以移除丢弃的字节
  void ReportCorruption(uint64_t bytes, const char* reason);
  void ReportDrop(uint64_t bytes, const Status& reason);

  SequentialFile* const file_;           // 源文件
  Reporter* const reporter_;             // 错误报告器
  bool const checksum_;                  // 是否验证校验和
  char* const backing_store_;            // 缓冲区存储
  Slice buffer_;                         // 当前缓冲区
  bool eof_;                            // 最后一次 Read() 是否表示 EOF（返回 < kBlockSize）

  // ReadRecord 返回的最后一条记录的偏移量
  // Offset of the last record returned by ReadRecord.
  uint64_t last_record_offset_;
  // buffer_ 结束位置之后的第一个位置的偏移量
  // Offset of the first location past the end of buffer_.
  uint64_t end_of_buffer_offset_;

  // 开始查找第一条返回记录的偏移量
  // Offset at which to start looking for the first record to return
  uint64_t const initial_offset_;

  // 是否在 seek 后重新同步（initial_offset_ > 0）
  // 在此模式下，可以静默跳过 kMiddleType 和 kLastType 记录
  // True if we are resynchronizing after a seek (initial_offset_ > 0). In
  // particular, a run of kMiddleType and kLastType records can be silently
  // skipped in this mode
  bool resyncing_;
};

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_READER_H_
