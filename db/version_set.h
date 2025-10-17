// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The representation of a DBImpl consists of a set of Versions.  The
// newest version is called "current".  Older versions may be kept
// around to provide a consistent view to live iterators.
//
// Each Version keeps track of a set of Table files per level.  The
// entire set of versions is maintained in a VersionSet.
//
// Version,VersionSet are thread-compatible, but require external
// synchronization on all accesses.

#ifndef STORAGE_LEVELDB_DB_VERSION_SET_H_
#define STORAGE_LEVELDB_DB_VERSION_SET_H_

#include <map>
#include <set>
#include <vector>

#include "db/dbformat.h"
#include "db/version_edit.h"
#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {

namespace log {
class Writer;
}

class Compaction;
class Iterator;
class MemTable;
class TableBuilder;
class TableCache;
class Version;
class VersionSet;
class WritableFile;

// Return the smallest index i such that files[i]->largest >= key.
// Return files.size() if there is no such file.
// REQUIRES: "files" contains a sorted list of non-overlapping files.
int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files, const Slice& key);

// Returns true iff some file in "files" overlaps the user key range
// [*smallest,*largest].
// smallest==nullptr represents a key smaller than all keys in the DB.
// largest==nullptr represents a key largest than all keys in the DB.
// REQUIRES: If disjoint_sorted_files, files[] contains disjoint ranges
//           in sorted order.
bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const std::vector<FileMetaData*>& files,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key);

// Version类代表数据库在某一时刻的状态快照
// 每个Version维护着每个层级的SSTable文件集合
// 多个Version通过双向链表组织起来，最新的Version称为"current"
// 旧的Version可能会被保留，以便为正在进行的迭代器提供一致性视图
// Version是引用计数的，当没有迭代器使用时会被删除
class Version {
 public:
  // Get操作的统计信息
  struct GetStats {
    FileMetaData* seek_file;       // 需要进行seek的文件
    int seek_file_level;           // 该文件所在的层级
  };

  // 将此Version的所有迭代器追加到*iters中
  // 当这些迭代器合并在一起时，会产生此Version的内容
  // 要求：此version已被保存（参见VersionSet::SaveTo）
  void AddIterators(const ReadOptions&, std::vector<Iterator*>* iters);

  // 查找key对应的值。如果找到，将其存储在*val中并返回OK
  // 否则返回非OK状态。填充*stats统计信息
  // 要求：未持有锁
  Status Get(const ReadOptions&, const LookupKey& key, std::string* val,
             GetStats* stats);

  // 将"stats"添加到当前状态中
  // 如果可能需要触发新的compaction则返回true，否则返回false
  // 要求：已持有锁
  bool UpdateStats(const GetStats& stats);

  // 记录在指定internal key处读取的字节样本
  // 样本大约每config::kReadBytesPeriod字节采样一次
  // 如果可能需要触发新的compaction则返回true
  // 要求：已持有锁
  bool RecordReadSample(Slice key);

  // 引用计数管理（防止Version在活跃的迭代器使用时被删除）
  void Ref();    // 增加引用计数
  void Unref();  // 减少引用计数，计数为0时删除

  // 获取指定层级中与[begin, end]范围重叠的所有文件
  // begin为nullptr表示所有key之前的key
  // end为nullptr表示所有key之后的key
  void GetOverlappingInputs(
      int level,
      const InternalKey* begin,  // nullptr means before all keys
      const InternalKey* end,    // nullptr means after all keys
      std::vector<FileMetaData*>* inputs);

  // 如果指定层级中的某个文件与[*smallest_user_key,*largest_user_key]范围重叠，则返回true
  // smallest_user_key==nullptr表示比DB中所有key都小的key
  // largest_user_key==nullptr表示比DB中所有key都大的key
  bool OverlapInLevel(int level, const Slice* smallest_user_key,
                      const Slice* largest_user_key);

  // 返回应该放置新的memtable compaction结果的层级
  // 该结果覆盖范围[smallest_user_key,largest_user_key]
  int PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                 const Slice& largest_user_key);

  // 返回指定层级的文件数量
  int NumFiles(int level) const { return files_[level].size(); }

  // 返回描述此version内容的人类可读字符串
  std::string DebugString() const;

 private:
  friend class Compaction;
  friend class VersionSet;

  class LevelFileNumIterator;

  explicit Version(VersionSet* vset)
      : vset_(vset),
        next_(this),
        prev_(this),
        refs_(0),
        file_to_compact_(nullptr),
        file_to_compact_level_(-1),
        compaction_score_(-1),
        compaction_level_(-1) {}

  Version(const Version&) = delete;
  Version& operator=(const Version&) = delete;

  ~Version();

  // 为指定层级创建一个连接迭代器
  Iterator* NewConcatenatingIterator(const ReadOptions&, int level) const;

  // 对每个与user_key重叠的文件调用func(arg, level, f)
  // 按从新到旧的顺序遍历。如果func返回false，则停止调用
  // 要求：internal_key的用户部分 == user_key
  void ForEachOverlapping(Slice user_key, Slice internal_key, void* arg,
                          bool (*func)(void*, int, FileMetaData*));

  VersionSet* vset_;  // 此Version所属的VersionSet
  Version* next_;     // 链表中的下一个version
  Version* prev_;     // 链表中的前一个version
  int refs_;          // 此version的活跃引用计数

  // 每个层级的文件列表
  std::vector<FileMetaData*> files_[config::kNumLevels];

  // 基于seek统计信息（当某个文件被seek访问次数过多时）的下一个要compaction的文件
  FileMetaData* file_to_compact_;
  int file_to_compact_level_; // 记录该文件所在层级

  // 下一个应该进行compaction的层级及其compaction分数，注意和上面两个字段的区别
  // 这些字段由Finalize()初始化
  double compaction_score_;
  int compaction_level_;
};

// VersionSet管理数据库的所有Version
// 它维护一个Version的双向链表，最新的Version称为"current"
// VersionSet负责：
// 1. 管理MANIFEST文件（记录版本变更历史）
// 2. 分配文件编号
// 3. 选择需要compaction的文件
// 4. 应用版本编辑（VersionEdit）来创建新的Version
class VersionSet {
 public:
  VersionSet(const std::string& dbname, const Options* options,
             TableCache* table_cache, const InternalKeyComparator*);
  VersionSet(const VersionSet&) = delete;
  VersionSet& operator=(const VersionSet&) = delete;

  ~VersionSet();

  // 将*edit应用到当前version以形成新的描述符
  // 该描述符既保存到持久化状态，又安装为新的当前version
  // 在实际写入文件时会释放*mu
  // 要求：进入时持有*mu
  // 要求：没有其他线程并发调用LogAndApply()
  Status LogAndApply(VersionEdit* edit, port::Mutex* mu)
      EXCLUSIVE_LOCKS_REQUIRED(mu);

  // 从持久化存储中恢复最后保存的描述符
  Status Recover(bool* save_manifest);

  // 返回当前version
  Version* current() const { return current_; }

  // 返回当前manifest文件编号
  uint64_t ManifestFileNumber() const { return manifest_file_number_; }

  // 分配并返回一个新的文件编号
  uint64_t NewFileNumber() { return next_file_number_++; }

  // 安排重用"file_number"，除非已经分配了更新的文件编号
  // 要求："file_number"由NewFileNumber()调用返回
  void ReuseFileNumber(uint64_t file_number) {
    if (next_file_number_ == file_number + 1) {
      next_file_number_ = file_number;
    }
  }

  // 返回指定层级的Table文件数量
  int NumLevelFiles(int level) const;

  // 返回指定层级所有文件的总大小
  int64_t NumLevelBytes(int level) const;

  // 返回最后的序列号
  uint64_t LastSequence() const { return last_sequence_; }

  // 设置最后的序列号为s
  void SetLastSequence(uint64_t s) {
    assert(s >= last_sequence_);
    last_sequence_ = s;
  }

  // 标记指定的文件编号为已使用
  void MarkFileNumberUsed(uint64_t number);

  // 返回当前日志文件编号
  uint64_t LogNumber() const { return log_number_; }

  // 返回当前正在被compaction的日志文件编号
  // 如果没有这样的日志文件则返回0
  uint64_t PrevLogNumber() const { return prev_log_number_; }

  // 为新的compaction选择层级和输入文件
  // 如果没有需要进行的compaction则返回nullptr
  // 否则返回一个堆分配的对象来描述compaction
  // 调用者应该删除结果
  Compaction* PickCompaction();

  // 返回一个compaction对象，用于compaction指定层级中[begin,end]范围的数据
  // 如果该层级中没有与指定范围重叠的内容则返回nullptr
  // 调用者应该删除结果
  Compaction* CompactRange(int level, const InternalKey* begin,
                           const InternalKey* end);

  // 返回level >= 1的任何文件在下一层级的最大重叠数据（以字节为单位）
  int64_t MaxNextLevelOverlappingBytes();

  // 创建一个迭代器来读取"*c"的compaction输入
  // 调用者在不再需要时应删除该迭代器
  Iterator* MakeInputIterator(Compaction* c);

  // 如果某个层级需要compaction则返回true
  bool NeedsCompaction() const {
    Version* v = current_;
    return (v->compaction_score_ >= 1) || (v->file_to_compact_ != nullptr);
  }

  // 将所有活跃version中列出的所有文件添加到*live中
  // 也可能改变一些内部状态
  void AddLiveFiles(std::set<uint64_t>* live);

  // 返回version "v"中键"key"的数据在数据库中的近似偏移量
  uint64_t ApproximateOffsetOf(Version* v, const InternalKey& key);

  // 返回每个层级文件数量的人类可读的简短（单行）摘要
  // 使用*scratch作为后备存储
  struct LevelSummaryStorage {
    char buffer[100];
  };
  const char* LevelSummary(LevelSummaryStorage* scratch) const;

 private:
  class Builder;

  friend class Compaction;
  friend class Version;

  // 尝试重用现有的manifest文件
  bool ReuseManifest(const std::string& dscname, const std::string& dscbase);

  // 计算version v的compaction优先级
  void Finalize(Version* v);

  // 获取inputs中所有文件的key范围
  void GetRange(const std::vector<FileMetaData*>& inputs, InternalKey* smallest,
                InternalKey* largest);

  // 获取inputs1和inputs2中所有文件的key范围
  void GetRange2(const std::vector<FileMetaData*>& inputs1,
                 const std::vector<FileMetaData*>& inputs2,
                 InternalKey* smallest, InternalKey* largest);

  // 为compaction设置其他输入文件
  void SetupOtherInputs(Compaction* c);

  // 将当前内容保存到*log
  Status WriteSnapshot(log::Writer* log);

  // 将version v追加到version链表中
  void AppendVersion(Version* v);

  Env* const env_;                    // 环境接口
  const std::string dbname_;          // 数据库名称
  const Options* const options_;      // 数据库选项
  TableCache* const table_cache_;     // Table缓存
  const InternalKeyComparator icmp_;  // 内部key比较器
  uint64_t next_file_number_;         // 下一个可用的文件编号
  uint64_t manifest_file_number_;     // manifest文件编号
  uint64_t last_sequence_;            // 最后的序列号
  uint64_t log_number_;               // 当前日志文件编号
  uint64_t prev_log_number_;          // 0或正在被compaction的memtable的后备存储

  // 延迟打开
  WritableFile* descriptor_file_;     // manifest文件
  log::Writer* descriptor_log_;       // manifest文件的日志写入器
  Version dummy_versions_;            // 哨兵节点，充当双向循环链表的头节点，简化插入和删除操作
  Version* current_;                  // 当前version，== dummy_versions_.prev_

  // 每个层级下一次compaction应该开始的key
  // 要么是空字符串，要么是有效的InternalKey
  std::string compact_pointer_[config::kNumLevels];
};

// A Compaction encapsulates information about a compaction.
class Compaction {
 public:
  ~Compaction();

  // Return the level that is being compacted.  Inputs from "level"
  // and "level+1" will be merged to produce a set of "level+1" files.
  int level() const { return level_; }

  // Return the object that holds the edits to the descriptor done
  // by this compaction.
  VersionEdit* edit() { return &edit_; }

  // "which" must be either 0 or 1
  int num_input_files(int which) const { return inputs_[which].size(); }

  // Return the ith input file at "level()+which" ("which" must be 0 or 1).
  FileMetaData* input(int which, int i) const { return inputs_[which][i]; }

  // Maximum size of files to build during this compaction.
  uint64_t MaxOutputFileSize() const { return max_output_file_size_; }

  // Is this a trivial compaction that can be implemented by just
  // moving a single input file to the next level (no merging or splitting)
  bool IsTrivialMove() const;

  // Add all inputs to this compaction as delete operations to *edit.
  void AddInputDeletions(VersionEdit* edit);

  // Returns true if the information we have available guarantees that
  // the compaction is producing data in "level+1" for which no data exists
  // in levels greater than "level+1".
  bool IsBaseLevelForKey(const Slice& user_key);

  // Returns true iff we should stop building the current output
  // before processing "internal_key".
  bool ShouldStopBefore(const Slice& internal_key);

  // Release the input version for the compaction, once the compaction
  // is successful.
  void ReleaseInputs();

 private:
  friend class Version;
  friend class VersionSet;

  Compaction(const Options* options, int level);

  int level_;
  uint64_t max_output_file_size_;
  Version* input_version_;
  VersionEdit edit_;

  // Each compaction reads inputs from "level_" and "level_+1"
  std::vector<FileMetaData*> inputs_[2];  // The two sets of inputs

  // State used to check for number of overlapping grandparent files
  // (parent == level_ + 1, grandparent == level_ + 2)
  std::vector<FileMetaData*> grandparents_;
  size_t grandparent_index_;  // Index in grandparent_starts_
  bool seen_key_;             // Some output key has been seen
  int64_t overlapped_bytes_;  // Bytes of overlap between current output
                              // and grandparent files

  // State for implementing IsBaseLevelForKey

  // level_ptrs_ holds indices into input_version_->levels_: our state
  // is that we are positioned at one of the file ranges for each
  // higher level than the ones involved in this compaction (i.e. for
  // all L >= level_ + 2).
  size_t level_ptrs_[config::kNumLevels];
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_SET_H_
