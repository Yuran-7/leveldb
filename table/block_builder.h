// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

/*
BlockBuilder 是“写者”或“创建者”: 它负责从零开始构建一个块的内容，将键值对编码成 LevelDB 的块格式
Block 是“读者”或“解析器”: 它负责解释一个已经存在的、已编码的块的字节序列，并提供遍历其中键值对的能力
*/
#ifndef STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_

#include <cstdint>
#include <vector>

#include "leveldb/slice.h"

namespace leveldb {

struct Options;

class BlockBuilder {
 public:
  explicit BlockBuilder(const Options* options);

  BlockBuilder(const BlockBuilder&) = delete;
  BlockBuilder& operator=(const BlockBuilder&) = delete;

  // Reset the contents as if the BlockBuilder was just constructed.
  void Reset();

  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  void Add(const Slice& key, const Slice& value);

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  Slice Finish();

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  size_t CurrentSizeEstimate() const;

  // Return true iff no entries have been added since the last Reset()
  bool empty() const { return buffer_.empty(); }

 private:
  const Options* options_;
  std::string buffer_;              // 实际的块数据（编码后的键值对、重启点数组、重启点数量）都存储在这个字符串中
  std::vector<uint32_t> restarts_;  // Restart points
  int counter_;                     // 计数器，记录自上一个重启点以来已发出的（添加的）条目数量
  bool finished_;                   // Has Finish() been called?
  std::string last_key_;            // 存储最后一次添加到块中的键。用于与下一个要添加的键进行比较，以实现前缀压缩
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
