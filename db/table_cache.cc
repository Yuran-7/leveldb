// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"

namespace leveldb {

// 用于存储在缓存中的值。
// 封装了SSTable文件本身和解析后的Table对象。
struct TableAndFile {
  RandomAccessFile* file;
  Table* table;
};

// 缓存条目的删除器。当一个条目被从缓存中驱逐时，此函数被调用。
// 它负责释放Table和RandomAccessFile资源。
static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

// 用于迭代器的清理函数。当迭代器被销毁时，此函数被调用。
// 它会释放缓存句柄，从而减少对应缓存条目的引用计数。
static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string& dbname, const Options& options,
                       int entries)
    : env_(options.env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)) {} // 初始化一个LRU缓存，entries大小990

TableCache::~TableCache() { delete cache_; }

// 查找或打开一个SSTable。
// 如果SSTable已在缓存中，则直接返回其句柄。
// 否则，打开文件，解析成Table对象，存入缓存，然后返回句柄。
Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle) {
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf)); // 使用文件号作为缓存的key
  *handle = cache_->Lookup(key);  // handle引用计数加一

  // 缓存未命中
  if (*handle == nullptr) {
    std::string fname = TableFileName(dbname_, file_number);
    RandomAccessFile* file = nullptr;
    Table* table = nullptr;
    s = env_->NewRandomAccessFile(fname, &file);
    if (!s.ok()) {
      // 尝试旧的文件名格式（.sst），以保持向后兼容
      std::string old_fname = SSTTableFileName(dbname_, file_number);
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }
    if (s.ok()) {
      // 打开文件成功，现在解析文件内容以创建Table对象
      s = Table::Open(options_, file, file_size, &table);
    }

    if (!s.ok()) {
      assert(table == nullptr);
      delete file;
      // 我们不缓存错误结果，这样如果错误是暂时的（例如文件正在被修复），
      // 我们可以自动恢复。
    } else {
      // 成功打开并解析了Table，将其存入缓存
      TableAndFile* tf = new TableAndFile;
      tf->file = file;
      tf->table = table;
      // 插入缓存，charge为1，表示每个Table在缓存中占一个单位的容量。
      // 注册DeleteEntry作为删除器。
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  }
  return s;
}

// 为指定的SSTable创建一个新的迭代器。
Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number, uint64_t file_size,
                                  Table** tableptr) {
  if (tableptr != nullptr) {
    *tableptr = nullptr;
  }

  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  // 从缓存句柄中获取Table对象
  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  // 创建Table的迭代器
  Iterator* result = table->NewIterator(options);
  // 注册一个清理函数。当这个迭代器被销毁时，UnrefEntry会被调用，
  // 从而释放我们刚刚从FindTable获得的缓存句柄。
  // 这是一种引用计数机制，确保只要有迭代器在使用Table，它就不会被从缓存中驱逐。
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != nullptr) {
    *tableptr = table;
  }
  return result;
}

// 在指定的SSTable中查找键k，如果找到则通过回调handle_result返回结果。
Status TableCache::Get(const ReadOptions& options, uint64_t file_number,
                       uint64_t file_size, const Slice& k, void* arg,
                       void (*handle_result)(void*, const Slice&,
                                             const Slice&)) {
  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, file_size, &handle);
  if (s.ok()) {
    // 从缓存句柄中获取Table对象
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    // 调用Table的内部Get方法执行查找
    s = t->InternalGet(options, k, arg, handle_result);
    // 查找操作完成，立即释放缓存句柄，减少引用计数。
    cache_->Release(handle);
  }
  return s;
}

// 从缓存中驱逐指定SSTable的条目。
// 这通常在SSTable文件被删除时调用。
void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace leveldb
