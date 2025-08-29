// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Log format information shared by reader and writer.
// See ../doc/log_format.md for more detail.

#ifndef STORAGE_LEVELDB_DB_LOG_FORMAT_H_
#define STORAGE_LEVELDB_DB_LOG_FORMAT_H_

namespace leveldb {
namespace log {

// 记录类型
enum RecordType {
  kZeroType = 0,    // 保留给预分配文件
  kFullType = 1,    // 完整记录
  kFirstType = 2,   // 分片记录的第一部分
  kMiddleType = 3,  // 分片记录的中间部分  
  kLastType = 4     // 分片记录的最后部分
};
static const int kMaxRecordType = kLastType;

static const int kBlockSize = 32768;    // 32KB 块大小

// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
static const int kHeaderSize = 4 + 2 + 1;   // 7字节头部

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_FORMAT_H_
