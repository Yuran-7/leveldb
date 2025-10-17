// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/cache.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>

#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace leveldb {

Cache::~Cache() {}

namespace {

// LRU 缓存实现
//
// 缓存条目有一个 "in_cache" 布尔值，指示缓存是否拥有对该条目的引用。
// 在不将条目传递给其 "deleter" 的情况下，此值变为 false 的唯一途径是：
// 通过 Erase()、通过 Insert() 插入具有重复键的元素时，或在缓存销毁时。
//
// 缓存维护两个缓存中条目的链表。缓存中的所有条目要么在一个链表中，要么在另一个链表中，
// 绝不会同时在两个链表中。仍被客户端引用但已从缓存中删除的条目不在任何一个链表中。
// 这两个链表是：
// - in-use: 包含当前被客户端引用的条目，顺序不固定。（此列表用于不变量检查。
//   如果我们移除该检查，否则会在此列表上的元素可能会被保留为断开的单例列表。）
// - LRU: 包含当前未被客户端引用的条目，按 LRU 顺序排列。
// 当 Ref() 和 Unref() 方法检测到缓存中的元素获得或失去其唯一的外部引用时，
// 元素会在这些列表之间移动。

// LRUHandle 是一个可变长度的堆分配结构，同时通过两种方式被组织
struct LRUHandle {
  void* value;                               // 真正的数据存储在这里
  void (*deleter)(const Slice&, void* value); // 用于删除值的函数指针
  LRUHandle* next_hash;                      // （哈希表）中下一个条目的指针（用于解决哈希冲突）
  LRUHandle* next;                           // （LRU 链表）中的下一个条目
  LRUHandle* prev;                           // （LRU 链表）中的上一个条目
  size_t charge;                             // 此条目占用的费用（例如内存大小）
  size_t key_length;                         // 键的长度
  bool in_cache;                             // 条目是否在缓存中
  uint32_t refs;                             // 引用计数，如果存在，则包括缓存的引用
  uint32_t hash;                             // key() 的哈希值；用于快速分片和比较
  char key_data[1];                          // 键的起始位置（柔性数组）

  Slice key() const {
    // 只有当 LRUHandle 是空列表的头节点时，next 才等于 this。
    // 列表头节点永远没有有意义的键。
    assert(next != this);

    return Slice(key_data, key_length);
  }
};

// 我们提供自己的简单哈希表，因为它消除了大量的移植 hack，
// 并且在我们测试过的一些编译器/运行时组合中，比一些内置的哈希表实现要快。
// 例如，readrandom 速度比 g++ 4.4.3 的内置哈希表快约 5%。
class HandleTable {
 public:
  HandleTable() : length_(0), elems_(0), list_(nullptr) { Resize(); }
  ~HandleTable() { delete[] list_; }

  LRUHandle* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }

  LRUHandle* Insert(LRUHandle* h) {
    LRUHandle** ptr = FindPointer(h->key(), h->hash);
    LRUHandle* old = *ptr;
    h->next_hash = (old == nullptr ? nullptr : old->next_hash);
    *ptr = h;
    if (old == nullptr) {
      ++elems_;
      if (elems_ > length_) {
        // 由于每个缓存条目都相当大，我们的目标是保持较短的平均链表长度（<= 1）。
        Resize();
      }
    }
    return old;
  }

  LRUHandle* Remove(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = FindPointer(key, hash);
    LRUHandle* result = *ptr;
    if (result != nullptr) {
      *ptr = result->next_hash;
      --elems_;
    }
    return result;
  }

 private:
  // 哈希表由一个桶数组组成，每个桶是一个哈希到该桶的缓存条目的链表。
  uint32_t length_; // 桶数组的长度
  uint32_t elems_;  // 哈希表中的元素数量
  LRUHandle** list_; // 桶数组，LRUHandle相对于一个Entry

  // 返回一个指向槽的指针，该槽指向与 key/hash 匹配的缓存条目。
  // 如果没有这样的缓存条目，则返回一个指向相应链表中尾随槽的指针。
  LRUHandle** FindPointer(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = &list_[hash & (length_ - 1)];
    while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
      ptr = &(*ptr)->next_hash;
    }
    return ptr;
  }

  void Resize() {
    uint32_t new_length = 4;
    while (new_length < elems_) {
      new_length *= 2;
    }
    LRUHandle** new_list = new LRUHandle*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; i++) {
      LRUHandle* h = list_[i];
      while (h != nullptr) {
        LRUHandle* next = h->next_hash;
        uint32_t hash = h->hash;
        LRUHandle** ptr = &new_list[hash & (new_length - 1)];
        h->next_hash = *ptr;
        *ptr = h;
        h = next;
        count++;
      }
    }
    assert(elems_ == count);
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
  }
};

// 分片缓存的单个分片。
class LRUCache {
 public:
  LRUCache();
  ~LRUCache();

  // 与构造函数分离，以便调用者可以轻松创建 LRUCache 数组
  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  // 类似于 Cache 的方法，但带有一个额外的 "hash" 参数。
  Cache::Handle* Insert(const Slice& key, uint32_t hash, void* value,
                        size_t charge,
                        void (*deleter)(const Slice& key, void* value));
  Cache::Handle* Lookup(const Slice& key, uint32_t hash);
  void Release(Cache::Handle* handle);
  void Erase(const Slice& key, uint32_t hash);
  void Prune();
  size_t TotalCharge() const {
    MutexLock l(&mutex_);
    return usage_;
  }

 private:
  void LRU_Remove(LRUHandle* e);
  void LRU_Append(LRUHandle* list, LRUHandle* e);
  void Ref(LRUHandle* e);
  void Unref(LRUHandle* e);
  bool FinishErase(LRUHandle* e) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // 使用前初始化。
  size_t capacity_;

  // mutex_ 保护以下状态。
  mutable port::Mutex mutex_;
  size_t usage_ GUARDED_BY(mutex_);

  // LRU 列表的虚拟头节点。
  // lru.prev 是最新条目，lru.next 是最旧条目。
  // 列表中的条目 refs==1 且 in_cache==true。
  LRUHandle lru_ GUARDED_BY(mutex_);

  // in-use 列表的虚拟头节点。
  // 列表中的条目正被客户端使用，refs >= 2 且 in_cache==true。
  LRUHandle in_use_ GUARDED_BY(mutex_);

  HandleTable table_ GUARDED_BY(mutex_);
};

LRUCache::LRUCache() : capacity_(0), usage_(0) {
  // 创建空的循环链表。
  lru_.next = &lru_;
  lru_.prev = &lru_;
  in_use_.next = &in_use_;
  in_use_.prev = &in_use_;
}

LRUCache::~LRUCache() {
  assert(in_use_.next == &in_use_);  // 如果调用者有未释放的句柄，则出错
  for (LRUHandle* e = lru_.next; e != &lru_;) {
    LRUHandle* next = e->next;
    assert(e->in_cache);
    e->in_cache = false;
    assert(e->refs == 1);  // lru_ 列表的不变量。
    Unref(e);
    e = next;
  }
}

void LRUCache::Ref(LRUHandle* e) {
  if (e->refs == 1 && e->in_cache) {  // 如果在 lru_ 列表上，则移动到 in_use_ 列表。
    LRU_Remove(e);
    LRU_Append(&in_use_, e);
  }
  e->refs++;
}

void LRUCache::Unref(LRUHandle* e) {
  assert(e->refs > 0);
  e->refs--;
  if (e->refs == 0) {  // 释放内存。
    assert(!e->in_cache);
    (*e->deleter)(e->key(), e->value);
    free(e);
  } else if (e->in_cache && e->refs == 1) {
    // 不再被使用；移动到 lru_ 列表。
    LRU_Remove(e);
    LRU_Append(&lru_, e);
  }
}

void LRUCache::LRU_Remove(LRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

void LRUCache::LRU_Append(LRUHandle* list, LRUHandle* e) {
  // 通过在 *list 之前插入，使 "e" 成为最新条目
  e->next = list;
  e->prev = list->prev;
  e->prev->next = e;
  e->next->prev = e;
}

Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  LRUHandle* e = table_.Lookup(key, hash);
  if (e != nullptr) {
    Ref(e);
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

void LRUCache::Release(Cache::Handle* handle) {
  MutexLock l(&mutex_);
  Unref(reinterpret_cast<LRUHandle*>(handle));
}

Cache::Handle* LRUCache::Insert(const Slice& key, uint32_t hash, void* value,
                                size_t charge,
                                void (*deleter)(const Slice& key,
                                                void* value)) {
  MutexLock l(&mutex_);

  // 分配空间，包含 LRUHandle 结构和 key 的数据
  LRUHandle* e =
      reinterpret_cast<LRUHandle*>(malloc(sizeof(LRUHandle) - 1 + key.size()));
  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->in_cache = false;
  e->refs = 1;  // 为返回的句柄增加引用计数。
  std::memcpy(e->key_data, key.data(), key.size());

  if (capacity_ > 0) {
    e->refs++;  // 为缓存的引用增加引用计数。
    e->in_cache = true;
    LRU_Append(&in_use_, e);
    usage_ += charge;
    FinishErase(table_.Insert(e));
  } else {  // 不缓存 (capacity_==0 是支持的，会关闭缓存功能)
    // next 在 key() 的断言中被读取，所以必须初始化
    e->next = nullptr;
  }
  // 如果当前使用量超过容量，并且 LRU 列表不为空，则淘汰最旧的条目
  while (usage_ > capacity_ && lru_.next != &lru_) {
    LRUHandle* old = lru_.next;
    assert(old->refs == 1);
    bool erased = FinishErase(table_.Remove(old->key(), old->hash));  // 从哈希表移除，从LRU链表移除，减少引用计数
    if (!erased) {  // 避免在编译 NDEBUG 时出现未使用变量的警告
      assert(erased);
    }
  }

  return reinterpret_cast<Cache::Handle*>(e);
}

// 如果 e != nullptr，完成从缓存中移除 *e 的操作；它已经从哈希表中移除了。
// 返回 e != nullptr。
bool LRUCache::FinishErase(LRUHandle* e) {
  if (e != nullptr) {
    assert(e->in_cache);
    LRU_Remove(e);
    e->in_cache = false;
    usage_ -= e->charge;
    Unref(e);
  }
  return e != nullptr;
}

void LRUCache::Erase(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  FinishErase(table_.Remove(key, hash));
}

void LRUCache::Prune() {
  MutexLock l(&mutex_);
  while (lru_.next != &lru_) {
    LRUHandle* e = lru_.next;
    assert(e->refs == 1);
    bool erased = FinishErase(table_.Remove(e->key(), e->hash));
    if (!erased) {  // 避免在编译 NDEBUG 时出现未使用变量的警告
      assert(erased);
    }
  }
}

// 分片数量的位数
static const int kNumShardBits = 4;
// 分片总数
static const int kNumShards = 1 << kNumShardBits;

// 分片LRU缓存
class ShardedLRUCache : public Cache {
 private:
  LRUCache shard_[kNumShards]; // LRU缓存分片数组
  port::Mutex id_mutex_;       // 用于生成新ID的互斥锁
  uint64_t last_id_;           // 上一个ID

  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);
  }

  static uint32_t Shard(uint32_t hash) { return hash >> (32 - kNumShardBits); }

 public:
  explicit ShardedLRUCache(size_t capacity) : last_id_(0) {
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity(per_shard);
    }
  }
  ~ShardedLRUCache() override {}
  Handle* Insert(const Slice& key, void* value, size_t charge,
                 void (*deleter)(const Slice& key, void* value)) override {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
  }
  Handle* Lookup(const Slice& key) override {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Lookup(key, hash);
  }
  void Release(Handle* handle) override {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].Release(handle);
  }
  void Erase(const Slice& key) override {
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key, hash);
  }
  void* Value(Handle* handle) override {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }
  uint64_t NewId() override {
    MutexLock l(&id_mutex_);
    return ++(last_id_);
  }
  void Prune() override {
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].Prune();
    }
  }
  size_t TotalCharge() const override {
    size_t total = 0;
    for (int s = 0; s < kNumShards; s++) {
      total += shard_[s].TotalCharge();
    }
    return total;
  }
};

}  // end anonymous namespace

Cache* NewLRUCache(size_t capacity) { return new ShardedLRUCache(capacity); }

}  // namespace leveldb
