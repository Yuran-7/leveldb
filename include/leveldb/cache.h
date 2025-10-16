// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Cache 是一个将键映射到值的接口。它具有内部同步机制，
// 可以安全地从多个线程并发访问。它可能会自动逐出条目以便为新条目腾出空间。
// 值会根据其“费用”（charge）计入缓存的总容量。例如，一个缓存，其值为可变长度的字符串，
// 可能会使用字符串的长度作为该字符串的费用。
//
// 提供了一个内置的、采用最近最少使用（LRU）淘汰策略的缓存实现。
// 如果客户端想要更复杂的实现（如抗扫描性、自定义淘汰策略、可变缓存大小等），
// 也可以使用自己的实现。

#ifndef STORAGE_LEVELDB_INCLUDE_CACHE_H_
#define STORAGE_LEVELDB_INCLUDE_CACHE_H_

#include <cstdint>

#include "leveldb/export.h"
#include "leveldb/slice.h"

namespace leveldb {

class LEVELDB_EXPORT Cache;

// 创建一个具有固定容量的新缓存。
// 此 Cache 的实现使用最近最少使用（LRU）的淘汰策略。
LEVELDB_EXPORT Cache* NewLRUCache(size_t capacity);

// Cache 类的抽象基类
class LEVELDB_EXPORT Cache {
 public:
  Cache() = default;

  Cache(const Cache&) = delete;
  Cache& operator=(const Cache&) = delete;

  // 析构函数。通过调用传递给构造函数的 "deleter" 函数来销毁所有现有的条目。
  virtual ~Cache();

  // 存储在缓存中条目的不透明句柄。
  // 用户通过这个句柄来引用缓存中的数据，而不是直接持有数据指针。
  struct Handle {};

  // 将一个从 key 到 value 的映射插入缓存，并为其分配指定的“费用”（charge），
  // 该费用将计入缓存的总容量。
  //
  // 返回一个与该映射相对应的句柄。当不再需要返回的映射时，
  // 调用者必须调用 this->Release(handle)。
  //
  // 当插入的条目不再需要时，其 key 和 value 将被传递给 "deleter" 函数进行清理。
  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) = 0;

  // 如果缓存中没有 "key" 对应的映射，则返回 nullptr。
  //
  // 否则，返回一个与该映射相对应的句柄。当不再需要返回的映射时，
  // 调用者必须调用 this->Release(handle)。
  virtual Handle* Lookup(const Slice& key) = 0;

  // 释放先前由 Lookup() 返回的映射。
  // 要求：handle 必须尚未被释放。
  // 要求：handle 必须是由 *this 上的方法返回的。
  virtual void Release(Handle* handle) = 0;

  // 返回由成功的 Lookup() 返回的句柄中封装的值。
  // 要求：handle 必须尚未被释放。
  // 要求：handle 必须是由 *this 上的方法返回的。
  virtual void* Value(Handle* handle) = 0;

  // 如果缓存中包含键为 key 的条目，则将其擦除。注意，底层的条目
  // 将会一直保留，直到所有指向它的现有句柄都被释放。
  virtual void Erase(const Slice& key) = 0;

  // 返回一个新的数字ID。可以被共享同一个缓存的多个客户端用来分区键空间。
  // 通常，客户端会在启动时分配一个新的ID，并将其前置到自己的缓存键上。
  virtual uint64_t NewId() = 0;

  // 移除所有未被活跃使用的缓存条目。内存受限的应用程序可能希望
  // 调用此方法以减少内存使用。
  // Prune() 的默认实现什么也不做。强烈建议子类覆盖默认实现。
  // leveldb 的未来版本可能会将 Prune() 更改为纯虚方法。
  virtual void Prune() {}

  // 返回存储在缓存中所有元素费用的总和估算值。
  virtual size_t TotalCharge() const = 0;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_CACHE_H_
