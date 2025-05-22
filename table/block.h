// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_H_

#include <cstddef>
#include <cstdint>

#include "leveldb/iterator.h"

namespace leveldb {

struct BlockContents;
class Comparator;

class Block {
 public:
  // Initialize the block with the specified contents.
  explicit Block(const BlockContents& contents);

  Block(const Block&) = delete;
  Block& operator=(const Block&) = delete;

  ~Block();

  size_t size() const { return size_; }
  Iterator* NewIterator(const Comparator* comparator);

 private:
  class Iter;

  uint32_t NumRestarts() const;

  const char* data_;  // 整个Block在内存中的起始地址
  size_t size_;
  uint32_t restart_offset_;  // Offset in data_ of restart array
  bool owned_;               // Block owns data_[]
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_H_

/*
<------------------------------------ size_ ------------------------------->
+--------------------------------------------------------------------------+
| data_ (指向这里)                                                         |
+--------------------------------------------------------------------------+
| Entry 1 (Key-Value Pair, 编码格式)                                       |
|   - shared_bytes (varint)                                                |
|   - unshared_bytes (varint)                                              |
|   - value_length (varint)                                                |
|   - key_delta (unshared_bytes 长度的字符)                                |
|   - value (value_length 长度的字符)                                      |
+--------------------------------------------------------------------------+
| Entry 2                                                                  |
|   ... (类似 Entry 1 的结构, key_delta 可能与 Entry 1 的 key 有共享前缀)  |
+--------------------------------------------------------------------------+
| ...                                                                      |
+--------------------------------------------------------------------------+
| Entry N                                                                  |
|   ...                                                                    |
+--------------------------------------------------------------------------+
|                                                                          |
| <------------------- restart_offset_ ------------------->               |
|                                                                          |
+==========================================================================+
| Restart Point 0 (uint32_t: 指向 Block 内某个 Entry 的偏移量)             |  <-- data_ + restart_offset_ 指向这里
+--------------------------------------------------------------------------+
| Restart Point 1 (uint32_t: 指向 Block 内某个 Entry 的偏移量)             |
+--------------------------------------------------------------------------+
| ...                                                                      |
+--------------------------------------------------------------------------+
| Restart Point M-1 (uint32_t: 指向 Block 内某个 Entry 的偏移量)           |
+--------------------------------------------------------------------------+
| Number of Restart Points (M) (uint32_t)                                  |
+--------------------------------------------------------------------------+
*/