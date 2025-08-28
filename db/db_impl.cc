// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <set>
#include <string>
#include <vector>

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"  // WAL读取器
#include "db/log_writer.h"  // WAL写入器，其实db_impl.h 第 14 行：已经包含了 #include "db/log_writer.h"，所以这里可以省略
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"   // 用户能看懂的LOG
#include "util/mutexlock.h"

namespace leveldb {

const int kNumNonTableCacheFiles = 10;  // .log，MANIFEST，CURRENT，LOCK等文件数

// 一个写入线程一个Writer实例
struct DBImpl::Writer {
  explicit Writer(port::Mutex* mu)
      : batch(nullptr), sync(false), done(false), cv(mu) {}

  Status status;
  WriteBatch* batch;  // 定义在write_batch.h中
  bool sync;
  bool done;
  port::CondVar cv;
};
// 合并期间，跟踪和管理该过程中的状态信息和产生的输出
struct DBImpl::CompactionState {
  // Compaction过程中产生的文件`元数据`
  struct Output {
    uint64_t number;    // 输出SSTable的文件编号
    uint64_t file_size; // 输出SSTable的文件大小
    InternalKey smallest, largest;  // 输出SSTable中的最小键和最大键
  };

  Output* current_output() { return &outputs[outputs.size() - 1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        smallest_snapshot(0),
        outfile(nullptr),
        builder(nullptr),
        total_bytes(0) {}

  Compaction* const compaction; // 指向描述本次compaction任务的Compaction对象

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot; // 最小的快照序列号。所有小于此序列号的旧版本数据，在满足一定条件下可以被丢弃

  std::vector<Output> outputs;  // 1次SST merge可能会写出多个SST

  // State kept for output being generated
  WritableFile* outfile;    // 指向当前正在写入的SSTable文件的指针
  TableBuilder* builder;    // 用于构建当前SSTable的TableBuilder对象指针
  uint64_t total_bytes; // 本次compaction所有输出文件累计写入的总字节数
};

// 用于规范化用户提供的 LevelDB 配置选项，确保它们在合理的范围内，并设置一些内部必需的组件
// 确保*ptr的值在[minvalue, maxvalue]的闭区间内
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}
// sanitized：审查，净化
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src; // 拷贝用户提供的选项
  result.comparator = icmp;
  // 如果用户提供了过滤器策略，则使用内部过滤器策略；否则不使用过滤器
  result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;
  ClipToRange(&result.max_open_files, 64 + kNumNonTableCacheFiles, 50000);  // kNumNonTableCacheFiles 为其他用途保留的文件数
  ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30);    // 64KB ~ 1GB
  ClipToRange(&result.max_file_size, 1 << 20, 1 << 30); // 1MB ~ 1GB
  ClipToRange(&result.block_size, 1 << 10, 4 << 20);    // 1KB ~ 4MB
  if (result.info_log == nullptr) {
    // 如果用户没有提供info_log，则尝试在数据库目录中创建一个默认的日志文件
    src.env->CreateDir(dbname);  // 确保数据库目录存在
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));   // 将旧的INFO_LOG文件重命名
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);   // 创建新的日志记录器
    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = nullptr;    // 如果创建失败，则info_log保持为nullptr
    }
  }
  // 如果用户没有提供block_cache，则创建一个默认的LRU缓存 (8MB)
  if (result.block_cache == nullptr) {
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}
// 计算表缓存（TableCache）可以使用的最大文件数，表缓存就可以理解SSTable缓存
static int TableCacheSize(const Options& sanitized_options) {
  // Reserve ten files or so for other uses and give the rest to TableCache.
  return sanitized_options.max_open_files - kNumNonTableCacheFiles;
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),  // Env
      internal_comparator_(raw_options.comparator), // InternalKeyComparator
      internal_filter_policy_(raw_options.filter_policy), // InternalFilterPolicy
      options_(SanitizeOptions(dbname, &internal_comparator_,
                               &internal_filter_policy_, raw_options)),
      owns_info_log_(options_.info_log != raw_options.info_log),  // bool
      owns_cache_(options_.block_cache != raw_options.block_cache), // bool
      dbname_(dbname),  // string
      table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_))),  // TableCache
      db_lock_(nullptr),  // FileLock
      shutting_down_(false),  // atomic<bool>
      background_work_finished_signal_(&mutex_),    // Mutex对象初始化CondVar对象
      mem_(nullptr),  // MemTable
      imm_(nullptr),  // MemTable
      has_imm_(false),  // atomic<bool>
      logfile_(nullptr),  // WritableFile
      logfile_number_(0),
      log_(nullptr),
      seed_(0), // uint32_t
      tmp_batch_(new WriteBatch),
      background_compaction_scheduled_(false),  // bool
      manual_compaction_(nullptr),  // ManualCompaction
      versions_(new VersionSet(dbname_, &options_, table_cache_,  // VersionSet
                               &internal_comparator_)) {}

DBImpl::~DBImpl() {
  // Wait for background work to finish.
  mutex_.Lock();
  // std::memory_order_release 是内存序，保证在这条语句之前对内存的写操作对其他线程可见，确保多线程同步的正确性。
  shutting_down_.store(true, std::memory_order_release);
  while (background_compaction_scheduled_) {
    background_work_finished_signal_.Wait();
  }
  mutex_.Unlock();

  if (db_lock_ != nullptr) {
    env_->UnlockFile(db_lock_);
  }

  delete versions_;
  if (mem_ != nullptr) mem_->Unref();
  if (imm_ != nullptr) imm_->Unref();
  delete tmp_batch_;
  delete log_;
  delete logfile_;
  delete table_cache_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }
}

// 当你首次创建一个 LevelDB 数据库时，这个 NewDB() 函数会被调用
Status DBImpl::NewDB() {
  VersionEdit new_db;
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0); // id>=0的log文件还没compact
  new_db.SetNextFile(2);
  new_db.SetLastSequence(0);

  const std::string manifest = DescriptorFileName(dbname_, 1);  // MANIFEST-000001
  WritableFile* file; // env.h中定义的
  Status s = env_->NewWritableFile(manifest, &file);    // 创建了这个 MANIFEST 文件并获取了可写文件句柄 file
  if (!s.ok()) {
    return s;
  }
  {
    log::Writer log(file);  // log是命名空间，Writer是log_writer.h中定义的类，它主要负责向 LevelDB 的日志文件（包括 MANIFEST 文件和 WAL 日志文件）写入记录
    std::string record;
    new_db.EncodeTo(&record);   // 将new_db对象的内容序列化到record字符串中
    s = log.AddRecord(record);  // 将序列化后的记录添加到MANIFEST文件中
    if (s.ok()) {
      s = file->Sync(); // 确保这个 MANIFEST 文件（包含了数据库的初始元数据）确实已经安全地存储到了磁盘上
    }
    if (s.ok()) {
      s = file->Close();
    }
  }
  delete file;
  if (s.ok()) {
    s = SetCurrentFile(env_, dbname_, 1); // 创建或更新数据库目录下的 CURRENT 文件，使其指向编号为 1 的 MANIFEST 文件
  } else {
    env_->RemoveFile(manifest);
  }
  return s;
}

// paranoid 多疑的，默认false
void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

// 主要作用是删除 LevelDB 数据库目录中不再需要的旧文件。这些文件可能包括：旧的日志文件 (log files)；旧的sstable；旧的 MANIFEST
void DBImpl::RemoveObsoleteFiles() {
  mutex_.AssertHeld();

  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // 获取所有需要保留的文件列表
  std::set<uint64_t> live = pending_outputs_; // 正在生成的SSTable文件
  versions_->AddLiveFiles(&live); // 当前所有版本引用的SSTable文件和MANIFEST文件

  std::vector<std::string> filenames;
  // 获取数据库目录下的所有文件名
  env_->GetChildren(dbname_, &filenames);  // Ignoring errors on purpose
  uint64_t number;
  FileType type;
  std::vector<std::string> files_to_delete;
  // 遍历目录下的所有文件
  for (std::string& filename : filenames) {
    // 解析文件名，获取文件编号和类型
    if (ParseFileName(filename, &number, &type)) {
      bool keep = true;
      switch (type) {
        case kLogFile:
          // 保留大于等于当前日志文件编号的日志文件，以及大于等于前一个日志文件编号的日志文件
          keep = ((number >= versions_->LogNumber()) ||
                  (number == versions_->PrevLogNumber()));
          break;
        case kDescriptorFile:
          // Keep my manifest file, and any newer incarnations'
          // (in case there is a race that allows other incarnations)
          // MANIFEST 文件，如果其编号在 live 集合中，则保留
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          // SSTable 文件，如果其编号在 live 集合中，则保留
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          // 临时文件，如果其编号在 live 集合中，则保留 (通常是正在生成的SSTable)
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:  // 旧的 InfoLog 文件也可能被删除，但通常有单独的逻辑处理
          keep = true;  // 这些文件总是保留
          break;
      }

      if (!keep) {  // 如果文件不需要保留
        files_to_delete.push_back(std::move(filename)); // 将其文件名添加到待删除列表
        if (type == kTableFile) { // 如果是SSTable文件类型
          // 从TableCache中驱逐该文件的条目，因为文件即将被删除，缓存它已无意义
          table_cache_->Evict(number);
        }
        Log(options_.info_log, "Delete type=%d #%lld\n", static_cast<int>(type),
            static_cast<unsigned long long>(number));
      }
    }
  }

  // While deleting all files unblock other threads. All files being deleted
  // have unique names which will not collide with newly created files and
  // are therefore safe to delete while allowing other threads to proceed.
  // 文件删除是I/O操作，可能耗时，解锁可以避免阻塞其他数据库操作
  mutex_.Unlock();
  for (const std::string& filename : files_to_delete) {
    env_->RemoveFile(dbname_ + "/" + filename); // 实际执行文件删除操作
  }
  mutex_.Lock();  // 临时释放的锁要重新加回去
}
// 函数在 LevelDB 数据库打开时被调用，负责恢复数据库到一致的状态
Status DBImpl::Recover(VersionEdit* edit, bool* save_manifest) {  // 是否需要写入新的manifest
  mutex_.AssertHeld();

  env_->CreateDir(dbname_); // env_是DBImpl的成员变量，指向一个Env对象，负责文件系统操作，函数定义在util/env_posix.cc，如果已经有同名文件夹，被视为“目录已存在”，不是致命错误
  assert(db_lock_ == nullptr);  // db_lock是DBImpl的成员变量，类型是FileLock*
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);  // 获取数据库锁文件，防止其他进程同时修改数据库，如果没有锁文件，会先创建。
  if (!s.ok()) {
    return s;
  }

  if (!env_->FileExists(CurrentFileName(dbname_))) {  // 目录中没有current文件
    if (options_.create_if_missing) { // 如果允许不存在目录时创建新目录
      Log(options_.info_log, "Creating DB %s since it was missing.",
          dbname_.c_str()); // Log函数在env.cc，往LOG文件中写INFO日志
      s = NewDB();  // 调用 NewDB 初始化数据库，包括创建第一个 MANIFEST 文件和 Current 文件
      if (!s.ok()) {
        return s;
      }
    } else {  // 数据库不存在且不允许创建
      return Status::InvalidArgument(
          dbname_, "does not exist (create_if_missing is false)");
    }
  } else {  // CURRENT 文件存在，说明数据库已存在
    if (options_.error_if_exists) { // 如果选项要求在数据库已存在时报错
      return Status::InvalidArgument(dbname_,
                                     "exists (error_if_exists is true)");
    }
  }

  // 步骤 1： 恢复manifest到versionset.
  s = versions_->Recover(save_manifest);
  if (!s.ok()) {
    return s;
  }
  SequenceNumber max_sequence(0); // 用于记录从日志文件中恢复出来的最大序列号

  // 步骤 2: 重做 (replay) 日志文件
  // 从 MANIFEST 中记录的日志文件号之后的所有日志文件都需要被重做。
  // 这些日志文件可能包含了上次数据库关闭前未持久化到 SSTable 的写操作。

  // 注意：PrevLogNumber() 不再被新版本 LevelDB 使用，但为了兼容旧版本生成的数据库，
  // 这里仍然会关注它。
  const uint64_t min_log = versions_->LogNumber();
  const uint64_t prev_log = versions_->PrevLogNumber(); // MANIFEST 中记录的前一个日志文件号 (用于兼容)
  std::vector<std::string> filenames;
  s = env_->GetChildren(dbname_, &filenames); // 获取数据库目录下所有文件
  if (!s.ok()) {
    return s;
  }

  std::set<uint64_t> expected;
  versions_->AddLiveFiles(&expected); // 将当前所有“活跃”的（即数据库正在引用的）SST文件编号
  uint64_t number;  // 用于存储从文件名解析出的文件编号
  FileType type;  // 用于存储从文件名解析出的文件类型
  std::vector<uint64_t> logs; // 用于收集需要进行重放（replay）的日志文件编号
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      expected.erase(number);
      // 如果是日志文件，并且其编号大于等于 min_log (或等于 prev_log)，则加入待重放列表
      if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
        logs.push_back(number);
    }
  }
  // 如果不为空，意味着 MANIFEST 中记录的某些 SSTable 或 MANIFEST 文件在磁盘上找不到了，这通常表示数据库损坏
  if (!expected.empty()) {
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%d missing files; e.g.",
                  static_cast<int>(expected.size()));
    return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
  }

  // Recover in the order in which the logs were generated
  std::sort(logs.begin(), logs.end());
  for (size_t i = 0; i < logs.size(); i++) {
    s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,  // 重做log并记录到edit
                       &max_sequence);
    if (!s.ok()) {
      return s;
    }

    // The previous incarnation may not have written any MANIFEST
    // records after allocating this log number.  So we manually
    // update the file number allocation counter in VersionSet.
    versions_->MarkFileNumberUsed(logs[i]); // 如果 logs[i] 大于等于 VersionSet 当前的 next_file_number_，它就会更新 VersionSet 的 next_file_number_ 为 logs[i] + 1
    // 这样做是为了确保，在恢复过程完成后，当数据库需要创建新的文件（无论是日志文件还是 SSTable 文件）时，它分配的文件编号不会与磁盘上已经存在的、并且在恢复过程中被重放的日志文件发生冲突
  }

  // 在所有相关的日志文件都成功重放后，检查从日志中恢复出来的最大序列号 (max_sequence)
  if (versions_->LastSequence() < max_sequence) {
    versions_->SetLastSequence(max_sequence);
  }

  return Status::OK();
}
// log_number: 要恢复的日志文件的编号；last_log:指示这是否是待恢复的日志文件序列中的最后一个
// save_manifest: 默认是false，落盘到level-0 会被设置为true
// edit：replay过程中，需要刷到level-0时，新SSTable的元信息就会被添加到edit中
Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // null if options_.paranoid_checks==false
    void Corruption(size_t bytes, const Status& s) override { // Corrupti：损坏
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == nullptr ? "(ignoring error) " : ""), fname,
          static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != nullptr && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file; // env.h中的类
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : nullptr);
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true /*checksum*/, 0 /*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long)log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  int compactions = 0;
  MemTable* mem = nullptr;
  while (reader.ReadRecord(&record, &scratch) && status.ok()) {
    if (record.size() < 12) { // 最小情况是序列号8加操作计数4，正常情况还包括（多对）键值对
      reporter.Corruption(record.size(),
                          Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);  // Slice转成WriteBatch格式

    if (mem == nullptr) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem); // 把所有操作插入mem中，（跳表中存了完整的entry？）
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) +
                                    WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) { // 4MB
      compactions++;
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, nullptr);
      mem->Unref();
      mem = nullptr;
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
    }
  }

  delete file;

  // 查看是否可以重用最后一个日志文件
  // reuse_logs默认是false
  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
    assert(logfile_ == nullptr);
    assert(log_ == nullptr);
    assert(mem_ == nullptr);
    uint64_t lfile_size;
    // 尝试获取当前日志文件 (fname) 的大小，并以追加模式重新打开它
    if (env_->GetFileSize(fname, &lfile_size).ok() &&
        env_->NewAppendableFile(fname, &logfile_).ok()) {
      Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
      log_ = new log::Writer(logfile_, lfile_size);
      logfile_number_ = log_number; // 设置当前活动的日志文件编号为被重用的日志文件编号
      if (mem != nullptr) {
        mem_ = mem;
        mem = nullptr;
      } else {
        // mem can be nullptr if lognum exists but was empty.
        mem_ = new MemTable(internal_comparator_);
        mem_->Ref();
      }
    }
  }

  if (mem != nullptr) { // 如果局部变量 mem 仍然指向一个 MemTable
    // mem did not get reused; compact it.
    if (status.ok()) {
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, nullptr);
    }
    mem->Unref();
  }

  return status;
}

Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {
  mutex_.AssertHeld();
  const uint64_t start_micros = env_->NowMicros();  // 记录开始时间
  FileMetaData meta;  // 用于存储新生成的SSTable的元数据
  meta.number = versions_->NewFileNumber(); // 为新的SSTable分配一个唯一的文件编号
  pending_outputs_.insert(meta.number);   // 将新文件编号加入到正在生成的文件集合中，防止被误删
  Iterator* iter = mem->NewIterator();  // 创建一个迭代器，用于遍历MemTable中的数据；Iterator定义在include/leveldb/iterator.h，MemTableIterator定义在db/memtable.cc
  Log(options_.info_log, "Level-0 table #%llu: started",
      (unsigned long long)meta.number); // 开始落盘

  Status s;
  {
    mutex_.Unlock();
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta); // 位于db/builder.cc
    mutex_.Lock();
  }

  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long)meta.number, (unsigned long long)meta.file_size,
      s.ToString().c_str());  // 文件编号，大小和构建状态，落盘结束，虽然是level-0 table，但这个SST不一定在level 0，有可能在level 1 或 level 2
  delete iter;
  pending_outputs_.erase(meta.number);  // 从正在生成的文件集合中移除该文件编号

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;
  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();  // 获取SSTable中的最小用户键
    const Slice max_user_key = meta.largest.user_key(); // 获取SSTable中的最大用户键
    if (base != nullptr) {  // 如果提供了基础版本(base version)
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key); // key没有重叠的话，直接下推level到深层，最高下推2层
    }
    // 将新生成的SSTable的元数据添加到VersionEdit中，表示版本变更
    // 添加日志记录新表的目标层级
    Log(options_.info_log,
        "WriteLevel0Table: Outputting table #%llu (memtable flush) to level-%d. Context: %s.",
        (unsigned long long)meta.number, level,
        (base == nullptr ? "RecoverLogFile" : "CompactMemTable"));
    edit->AddFile(level, meta.number, meta.file_size, meta.smallest,  
                  meta.largest);
  }

  CompactionStats stats;    // 注意和CompactionState的区别，CompactionStats只是一些简单的统计信息
  stats.micros = env_->NowMicros() - start_micros;  // 总耗时
  stats.bytes_written = meta.file_size; // 写入字节数
  stats_[level].Add(stats); // 将统计信息添加到对应层级的统计数据中，DBImpl的成员变量
  return s;
}

// MaybeScheduleCompaction -> BGwork -> BackgroundCall -> BackgroundCompaction -> (PickCompaction, DoCompactionWork -> CompactMemTable)
void DBImpl::CompactMemTable() {
  mutex_.AssertHeld();
  assert(imm_ != nullptr);

  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();
  base->Ref();
  Status s = WriteLevel0Table(imm_, &edit, base);
  base->Unref();

  if (s.ok() && shutting_down_.load(std::memory_order_acquire)) { // 数据库正在关闭 (shutting_down_ 为 true)
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0); // 清除 PrevLogNumber，之前与 imm_ 对应的 WAL 日志段之前的日志就不再需要了
    edit.SetLogNumber(logfile_number_);  // Earlier logs no longer needed
    // 将 VersionEdit 中的变更应用到 VersionSet，并持久化到 MANIFEST 文件
    s = versions_->LogAndApply(&edit, &mutex_);
  }

  if (s.ok()) {
    // Commit to the new state
    imm_->Unref();
    imm_ = nullptr;
    has_imm_.store(false, std::memory_order_release); // 更新原子变量 has_imm_ 为 false，表示当前没有不可变的 memtable
    RemoveObsoleteFiles();  // 旧的日志文件、已被合并的 SSTable 文件
  } else {
    RecordBackgroundError(s);
  }
}

// 手动触发对数据库中指定键范围 [begin, end) 内的数据进行 Compaction，为nullptr时表示从最小键到最大键
// 这个函数会尝试将指定范围内的数据尽可能地合并到更深的层级
void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_); // 加锁以保护对 VersionSet 的并发访问
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {  // version_set.cc中定义
        max_level_with_files = level; // 如果存在重叠，则更新 max_level_with_files 为当前层级
      }
    }
  }
  // 调用 TEST_CompactMemTable() 来确保 MemTable 和 Immutable MemTable (如果存在且与范围重叠) 中的数据被刷到 Level-0
  // 这里的 "TEST" 命名很可能表明该函数提供了一种直接强制将 memtable（或多个 memtable）刷到磁盘的方式，而这个动作通常是由后台进程处理的
  TEST_CompactMemTable();  // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    // TEST_CompactRange 会尝试调度一次手动的 Compaction，将 level 层中与范围重叠的数据向下层合并
    TEST_CompactRange(level, begin, end);
  }
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin,
                               const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == nullptr) {
    manual.begin = nullptr;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == nullptr) {
    manual.end = nullptr;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.load(std::memory_order_acquire) &&
         bg_error_.ok()) {
    if (manual_compaction_ == nullptr) {  // Idle
      manual_compaction_ = &manual;
      MaybeScheduleCompaction();
    } else {  // Running either my compaction or another compaction.
      background_work_finished_signal_.Wait();
    }
  }
  // Finish current background compaction in the case where
  // `background_work_finished_signal_` was signalled due to an error.
  while (background_compaction_scheduled_) {
    background_work_finished_signal_.Wait();
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = nullptr;
  }
}

Status DBImpl::TEST_CompactMemTable() {
  // nullptr batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), nullptr);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != nullptr && bg_error_.ok()) {
      background_work_finished_signal_.Wait();
    }
    if (imm_ != nullptr) {
      s = bg_error_;
    }
  }
  return s;
}

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) { // 之前没有记录过后台错误
    bg_error_ = s;  // 如果 bg_error_ 之前是 OK 状态，则将其更新为传入的错误状态 s
    background_work_finished_signal_.SignalAll(); // 唤醒所有因等待后台工作完成（例如 Compaction）而被阻塞的线程
    //这样做是为了让这些线程能够感知到后台错误，并做出相应的处理（例如，停止等待并返回错误）
  }
}

// 这个方法在每次写入操作完成、数据库打开或后台任务空闲时被调用，用来检查是否需要进行 Compaction
// 如果需要，就会去执行 Compaction
// MaybeScheduleCompaction -> BGwork -> BackgroundCall -> BackgroundCompaction -> (PickCompaction, DoCompactionWork)
void DBImpl::MaybeScheduleCompaction() {
  mutex_.AssertHeld();
  if (background_compaction_scheduled_) { // 检查是否已经有一个后台 Compaction 任务被调度
    // Already scheduled
    // 直接返回，避免重复调度
  } else if (shutting_down_.load(std::memory_order_acquire)) {  // 读取原子变量 shutting_down_ 的当前值
    // DB is being deleted; no more background compactions
  } else if (!bg_error_.ok()) { // 检查是否已经发生了后台错误
    // Already got an error; no more changes
    // 如果已经存在后台错误（例如磁盘已满），则不应再安排新的 Compaction，因为它们也可能会失败
  } else if (imm_ == nullptr && manual_compaction_ == nullptr &&
             !versions_->NeedsCompaction()) {
    // No work to be done
    // 检查是否没有工作可做：
    // 1. imm_ == nullptr: 当前没有不可变的 MemTable 需要被 Compaction。
    // 2. manual_compaction_ == nullptr: 当前没有手动触发的 Compaction 请求。
    // 3. !versions_->NeedsCompaction(): VersionSet 判断当前 SSTable 的组织结构不需要进行自动 Compaction
  } else {
    background_compaction_scheduled_ = true;  // 任何时候最多只有一个后台 Compaction 任务被调度或正在执行
    env_->Schedule(&DBImpl::BGWork, this);  // this指向当前的 DBImpl 对象实例，this作为参数传递给 BGWork，新开一个线程执行压缩
  }
}

void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCall();  // 把指针强转为DBImpl类型
}

void DBImpl::BackgroundCall() {
  MutexLock l(&mutex_); // 构造函数内部调用这个 mutex_ 对象的 Lock() 方法，从而实现加锁
  assert(background_compaction_scheduled_);
  if (shutting_down_.load(std::memory_order_acquire)) {
    // No more background work when shutting down.
    // 如果数据库正在关闭，则不执行任何后台工作
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundCompaction(); // 来实际执行 compaction 操作（可能是 memtable compaction 或 sstable compaction）
  }

  background_compaction_scheduled_ = false;

  // Previous compaction may have produced too many files in a level,
  // so reschedule another compaction if needed.
  // 上一次的 compaction 可能导致某个层级的文件过多，或者有新的 imm_ memtable 产生，
  // 因此在这里再次调用 MaybeScheduleCompaction() 来检查是否需要立即调度下一次 compaction
  MaybeScheduleCompaction();
  // 唤醒所有可能因等待后台工作（如 compaction 或 memtable 刷盘）完成而被阻塞的线程。
  // 例如，写操作在 MakeRoomForWrite 中可能会因为 imm_ 存在或 L0 文件过多而等待
  background_work_finished_signal_.SignalAll(); // port::CondVar
}

void DBImpl::BackgroundCompaction() {
  mutex_.AssertHeld();

  if (imm_ != nullptr) {
    CompactMemTable();
    return;
  }

  Compaction* c;  // 定义在version_set.h中，表示一个压缩任务
  bool is_manual = (manual_compaction_ != nullptr);
  InternalKey manual_end; // 如果是手动 Compaction，用于记录本次 Compaction 实际处理到的结束键
  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    // 调用 VersionSet 的 CompactRange 方法，返回一个 Compaction 对象；注意void DBImpl::CompactRange(const Slice* begin, const Slice* end)是给用户调用的
    c = versions_->CompactRange(m->level, m->begin, m->end);
    m->done = (c == nullptr); // 如果 CompactRange 未返回有效的 Compaction 任务，说明手动 Compaction 完成或无法进行；这里的 m 和成员变量 manual_compaction_ 以及 manual 三者是相互关联的
    if (c != nullptr) {
      // 记录该任务输入文件中的最大键，作为手动 Compaction 的一个分界点
      // 手动 Compaction 可能因为范围过大而分多次进行
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;
    }
    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level, (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else {  // 如果不是手动 Compaction，则进行自动 Compaction
    c = versions_->PickCompaction();  // 根据最新version，决策要compact哪些sst，三层
  }

  Status status;
  if (c == nullptr) {
    // Nothing to do
  } else if (!is_manual && c->IsTrivialMove()) {  // Trivial Move：L层sst个数为1，且L+1层sst个数为0，且 TotalFileSize(grandparents_) <=MaxGrandParentOverlapBytes(vset->options_))
    // Move file to next level
    assert(c->num_input_files(0) == 1); // 平凡移动只涉及一个输入文件
    FileMetaData* f = c->input(0, 0); // 获取这个输入文件的元数据
    c->edit()->RemoveFile(c->level(), f->number); // 在 VersionEdit 中记录删除 L 层的文件
    c->edit()->AddFile(c->level() + 1, f->number, f->file_size, f->smallest,
                       f->largest); // 在 VersionEdit 中记录将该文件添加到 L+1 层
    status = versions_->LogAndApply(c->edit(), &mutex_);  // // 将 VersionEdit 应用到 VersionSet，并持久化到 MANIFEST 文件，这里是简单的sst层级腾挪，没有真的IO操作sst
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;  // 用于存储层级摘要信息，供日志记录
    Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
        static_cast<unsigned long long>(f->number), c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str(), versions_->LevelSummary(&tmp));
  } else {  // sst和下一层sst有overlay，做merge
    CompactionState* compact = new CompactionState(c);  // 根据 Compaction 对象创建一个 CompactionState 对象；CompactionState > Compaction
    status = DoCompactionWork(compact); // 调用 DoCompactionWork 函数执行实际的 Compaction 工作（读取、合并、写入 SSTable）
    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    CleanupCompaction(compact); // 清理 CompactionState 对象和相关资源
    c->ReleaseInputs(); // 清理Version* input_version_;
    RemoveObsoleteFiles();  // Compaction 完成后，删除不再需要的旧文件（例如被合并掉的输入 SSTable）
  }
  delete c; // 删除 Compaction 对象

  if (status.ok()) {
    // Done
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log, "Compaction error: %s", status.ToString().c_str());
  }

  if (is_manual) {
    ManualCompaction* m = manual_compaction_;   // m指向DBImpl::manual_compaction_，TEST_CompactRange中manual_compaction_指向manual
    if (!status.ok()) {
      m->done = true; // 标记手动 Compaction 完成（尽管是失败的）
    }
    if (!m->done) { // m->done = (c == nullptr)，所以c = versions_->CompactRange(m->level, m->begin, m->end);只要不为空就会走这个if
      // 更新手动 Compaction 请求的起始键为本次 Compaction 实际处理到的结束键，
      // 以便下次可以继续 Compaction 剩余的范围
      m->tmp_storage = manual_end;
      m->begin = &m->tmp_storage;
    }
    manual_compaction_ = nullptr; // 清除当前的手动 Compaction 请求指针
  }
}

void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  // 如果 builder 不为空，说明可能在 Compaction 过程中发生了错误，导致 TableBuilder 未能正常完成
  if (compact->builder != nullptr) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();  // 放弃 TableBuilder 的当前构建，不会生成有效的 SSTable 文件
    delete compact->builder;
  } else {  // 如果 builder 为空，说明 TableBuilder 要么从未被创建，要么已经被正确处理和删除了，这种情况下，outfile 也应该为空
    assert(compact->outfile == nullptr);
  }
  delete compact->outfile;
  // pending_outputs_ 用于跟踪正在生成的 SSTable 文件，以防止它们被 RemoveObsoleteFiles 误删
  // Compaction 清理阶段，这些文件要么已经成功安装到新版本中，要么因为失败而被丢弃
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}

// 用在DoCompactionWork()中
Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {
  // compact 对象不为空，并且当前没有正在使用的 TableBuilder
  assert(compact != nullptr);
  assert(compact->builder == nullptr);
  uint64_t file_number; // 用于存储新分配的文件编号
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber(); // 从 VersionSet 获取一个新的唯一文件编号
    pending_outputs_.insert(file_number); // 将新文件编号添加到 pending_outputs_ 集合中，防止意外删除
    CompactionState::Output out;  // 创建一个新的 Output 对象来存储新输出文件的元数据
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);  // outputs 列表记录了本次 Compaction 产生的所有输出文件
    mutex_.Unlock();
  }

  // Make the output file
  std::string fname = TableFileName(dbname_, file_number);  // 根据数据库名和文件编号生成 SSTable 的完整文件名
  // compact->outfile 将指向这个新创建的 WritableFile 对象，用于后续的写入操作
  Status s = env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {
    // 创建一个新的 TableBuilder 对象，用于向 compact->outfile 写入数据
    compact->builder = new TableBuilder(options_, compact->outfile);
  }
  return s;
}

Status DBImpl::FinishCompactionOutputFile(CompactionState* compact,
                                          Iterator* input) {
  assert(compact != nullptr);
  assert(compact->outfile != nullptr);
  assert(compact->builder != nullptr);

  const uint64_t output_number = compact->current_output()->number; // 获取当前输出文件的编号
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status(); // 检查输入迭代器在之前的操作中是否发生了错误
  const uint64_t current_entries = compact->builder->NumEntries();  // 获取当前 TableBuilder 中已写入的条目数
  if (s.ok()) {
    s = compact->builder->Finish();
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();  // 获取构建完成（或放弃）后 SSTable 的文件大小
  compact->current_output()->file_size = current_bytes; // 更新 CompactionState 中当前输出文件的元数据：文件大小
  compact->total_bytes += current_bytes;  // 将当前文件的大小累加到本次 Compaction 输出的总字节数中
  delete compact->builder;  // 删除 TableBuilder 对象
  compact->builder = nullptr; // 将 builder 指针置空

  // Finish and check for file errors
  if (s.ok()) {
    s = compact->outfile->Sync();   // 如果之前的操作都成功，则将输出文件同步到磁盘，确保数据持久化
  }
  if (s.ok()) {
    s = compact->outfile->Close();  // 写到SST关闭SST
  }
  delete compact->outfile;  // 删除 WritableFile 对象
  compact->outfile = nullptr;

  if (s.ok() && current_entries > 0) {  // 如果所有操作都成功，并且确实向 SSTable 中写入了数据
    // Verify that the table is usable
    // 验证新生成的 SSTable 是否可用（这是一个可选的健全性检查）。
    // 通过 TableCache 创建一个迭代器来尝试读取这个新表。
    Iterator* iter =
        table_cache_->NewIterator(ReadOptions(), output_number, current_bytes);
    s = iter->status(); // 检查创建迭代器和读取表元数据的状态
    delete iter;
    if (s.ok()) {
      Log(options_.info_log, "Generated table #%llu@%d: %lld keys, %lld bytes",
          (unsigned long long)output_number, compact->compaction->level(),
          (unsigned long long)current_entries,
          (unsigned long long)current_bytes);
    }
  }
  return s;
}

// 将Compaction的结果（新生成的SSTable和被删除的旧SSTable）应用到数据库的元数据中，即生成并应用一个新的Version
// DoCompactionWork() 结束后会调用这个函数来更新数据库的状态
Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();

  Log(options_.info_log, "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0), compact->compaction->level(),  // level L的输入文件数量和层级
      compact->compaction->num_input_files(1), compact->compaction->level() + 1,  // level L+1的输入文件数量和层级
      static_cast<long long>(compact->total_bytes));  // Compaction输出的总字节数

  // 将所有参与Compaction的输入SSTable在VersionEdit中标记为待删除
  compact->compaction->AddInputDeletions(compact->compaction->edit());  // version的变更1）删除多个被合并的SST
  const int level = compact->compaction->level();
  for (size_t i = 0; i < compact->outputs.size(); i++) {  // version的变更2）合并出了N个SST
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(level + 1, out.number, out.file_size,
                                         out.smallest, out.largest);
  }
  return versions_->LogAndApply(compact->compaction->edit(), &mutex_);  // 产生新version
}

Status DBImpl::DoCompactionWork(CompactionState* compact) {
  const uint64_t start_micros = env_->NowMicros();  // 记录开始时间
  int64_t imm_micros = 0;  // 记录累计因处理imm_而花费的时间

  Log(options_.info_log, "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1), compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0); // 断言L层至少有一个文件参与Compaction
  assert(compact->builder == nullptr);  // 负责将输入的键值对按照 SSTable 的格式进行组织和构建
  assert(compact->outfile == nullptr);  // 代表 Compaction 过程中新生成的 SSTable 文件在文件系统中的实体
  if (snapshots_.empty()) { // 如果当前没有活跃的快照
    compact->smallest_snapshot = versions_->LastSequence(); // 使用数据库当前的最新序列号
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->sequence_number();  // 活跃快照列表的第一个的序列号（uint64_t）
  }
  // 创建一个合并迭代器 (MergingIterator)，用于按顺序遍历所有输入SSTable中的键值对
  // 内部为每个SSTable创建一个子迭代器（或者一层一个迭代器），外部只暴露一个统一的迭代器接口 input
  Iterator* input = versions_->MakeInputIterator(compact->compaction);

  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();  // compact过程是不需要锁的，因为current的最终引用在versionset手里，且current里面的sst是imutable的

  // 开始merge多个sst
  input->SeekToFirst(); // 定位到所有输入 SSTable 中全局最小的那个键上
  Status status;
  ParsedInternalKey ikey; // InternalKey通常是序列化后的字符串Slice，把user key、sequence number和value type拼接在一起；ParsedInternalKey是一个解包之后的结构体
  std::string current_user_key; // 当前正在处理的用户键
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;  // 记录当前用户键遇到的上一个序列号

  // 循环遍历所有输入键值对（所以sst），直到迭代器无效或数据库正在关闭
  while (input->Valid() && !shutting_down_.load(std::memory_order_acquire)) {
    // 优先处理不可变MemTable (imm_) 的Compaction
    if (has_imm_.load(std::memory_order_relaxed)) { // 检查是否有imm_需要处理
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();
      if (imm_ != nullptr) {
        CompactMemTable();  // 执行imm_的Compaction
        // 唤醒可能因等待imm_处理完毕而被阻塞的写操作
        background_work_finished_signal_.SignalAll();
      }
      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);  // 累加处理imm_所花费的时间
    }


    Slice key = input->key(); // 获取当前迭代器指向的InternalKey
    // 判断是否需要切分当前SSTable文件（可能还没到最大文件大小，但已到达分界点）
    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != nullptr) {
      status = FinishCompactionOutputFile(compact, input);  // 完成并关闭当前的输出SSTable；如果真要切割，当前的key不会被写入刚刚完成的SSTable中
      if (!status.ok()) {
        break;
      }
    }

    // Handle key/value, add to state, etc.
    bool drop = false;  // 标记是否应该丢弃当前的键值对
    if (!ParseInternalKey(key, &ikey)) {  // 解析InternalKey
      // Do not hide error keys
      // 解析失败
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      // 遇到新的ukey会进入if，因为是key是顺序遍历，所以可能上一个key和当前的key是一样的
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) != 0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());  // 更新当前用户键，assign 的作用是把 ikey.user_key 指向的内容（字节数组）复制到 current_user_key 这个 std::string 里，实现内容的拷贝。
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber; // 首次遇到ukey，没有上一次seq id
      }

      if (last_sequence_for_key <= compact->smallest_snapshot) {    // 如果snapshot是T时刻的，那么T-1以前的变更是允许合并掉的；都是SequenceNumber类型，也即uint64_t
        // Hidden by an newer entry for same user key
        drop = true;  // (A)
      } else if (ikey.type == kTypeDeletion &&      // 如果该ukey在下层(level_+2)都没出现过，那么这个del ukey记录也不必留存
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;  // 当前ukey上一次seq id
    }
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

    // 如果drop为true，就不会走这段逻辑，也即这个key被丢掉了；向合并后的SST插入这条entry
    if (!drop) {
      // Open output file if necessary
      if (compact->builder == nullptr) {
        status = OpenCompactionOutputFile(compact); // 1.分配一个新的SSTable文件编号；2.在磁盘上创建一个新的SSTable文件；3.初始化TableBuilder，准备后续把合并后的数据写入这个新文件
        if (!status.ok()) {
          break;
        }
      }
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);  // 记录SST写入的第1个key（也是最小的Key）
      }
      compact->current_output()->largest.DecodeFrom(key); // 更新SST的最后1个key（也是最大的Key）
      compact->builder->Add(key, input->value()); // 把当前遍历到的key和value写入到正在构建的SSTable文件中

      // Close output file if it is big enough
      if (compact->builder->FileSize() >=
          compact->compaction->MaxOutputFileSize()) { // 当前SST大于max_output_file_size_大小
        status = FinishCompactionOutputFile(compact, input);  // 1.完成文件写入；2.关闭当前SSTable文件；3.记录元数据；4.清理状态，为下次写入做准备
        if (!status.ok()) {
          break;
        }
      }
    }

    input->Next();  // 移动到下一个输入键值对
  } // while

  if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
    status = Status::IOError("Deleting DB during compaction");
  }
  if (status.ok() && compact->builder != nullptr) {
    status = FinishCompactionOutputFile(compact, input);  // 剩余在builder中的数据再生成一个SSTable文件
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = nullptr;

  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;
  for (int which = 0; which < 2; which++) { // which 取0和1，分别表示本次Compaction的两个输入层（如level和level+1）
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;  // 遍历每个输入层的所有SSTable文件，把它们的文件大小累加到stats.bytes_read
    }
  }
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }

  mutex_.Lock();
  stats_[compact->compaction->level() + 1].Add(stats);  // 把本次Compaction的统计信息（如耗时、读写字节数等）累加到目标输出层（level+1）的统计数据中

  if (status.ok()) {
    status = InstallCompactionResults(compact); // 1.把新生成的 SSTable 文件加入到数据库的版本信息（VersionSet）中；2.把本次合并过程中需要删除的旧 SSTable 文件从元数据中移除；3.生成并应用一个新的 Version，确保数据库后续操作能看到最新的文件布局。
  }
  if (!status.ok()) {
    RecordBackgroundError(status);
  }
  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log, "compacted to: %s", versions_->LevelSummary(&tmp));
  return status;
}

namespace {

struct IterState {
  port::Mutex* const mu;
  Version* const version GUARDED_BY(mu);
  MemTable* const mem GUARDED_BY(mu);
  MemTable* const imm GUARDED_BY(mu);

  IterState(port::Mutex* mutex, MemTable* mem, MemTable* imm, Version* version)
      : mu(mutex), version(version), mem(mem), imm(imm) {}
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != nullptr) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}

}  // anonymous namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());  // MemTableIterator
  mem_->Ref();
  if (imm_ != nullptr) {
    list.push_back(imm_->NewIterator());    // MemTableIterator
    imm_->Ref();
  }
  versions_->current()->AddIterators(options, &list);   // level 0每个SST一个TwoLevelIterator，其他层只要一个
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size()); // MergingIterator
  versions_->current()->Ref();

  IterState* cleanup = new IterState(&mutex_, mem_, imm_, versions_->current());
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);

  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

Status DBImpl::Get(const ReadOptions& options, const Slice& key,
                   std::string* value) {
  Status s;
  MutexLock l(&mutex_); // 加锁，保护共享数据结构，如 mem_, imm_, versions_ 等
  SequenceNumber snapshot;  // uint64_t
  // 如果 ReadOptions 指定了快照（用户指定的），则使用该快照的序列号；否则，使用当前数据库的最新序列号作为快照
  if (options.snapshot != nullptr) {
    snapshot =
        static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
  } else {
    snapshot = versions_->LastSequence();
  }

  MemTable* mem = mem_;
  MemTable* imm = imm_;
  Version* current = versions_->current();
  mem->Ref();
  if (imm != nullptr) imm->Ref();
  current->Ref();

  bool have_stat_update = false;  // 标记是否从 SSTable 中读取了数据，从而可能需要更新文件的统计信息
  Version::GetStats stats;  // 用于收集读取操作的统计信息，例如哪些文件被访问了

  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    // 构建查找键 (LookupKey)，它包含了用户键 (key) 和快照序列号 (snapshot)
    LookupKey lkey(key, snapshot);
    if (mem->Get(lkey, value, &s)) {  // 1. 首先在可变的 MemTable (mem_) 中查找
      // Done
    } else if (imm != nullptr && imm->Get(lkey, value, &s)) { // 2. 如果 MemTable 中未找到，且存在不可变的 MemTable (imm_)，则在 imm_ 中查找
      // Done
    } else {  // current Version下面的sstable检索
      s = current->Get(options, lkey, value, &stats);
      have_stat_update = true;  // 标记从 SSTable 中进行了查找，需要更新文件访问统计
    }
    mutex_.Lock();
  }
  // 如果从 SSTable 中进行了查找 (have_stat_update 为 true)，并且 Version 的统计信息确实需要更新 (UpdateStats 返回 true)，
  // (例如，某个文件被频繁访问，可能触发 Compaction)，则尝试调度一次 Compaction
  if (have_stat_update && current->UpdateStats(stats)) {
    MaybeScheduleCompaction();
  }
  mem->Unref();
  if (imm != nullptr) imm->Unref();
  current->Unref();
  return s;
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
  return NewDBIterator(this, user_comparator(), iter,
                       (options.snapshot != nullptr
                            ? static_cast<const SnapshotImpl*>(options.snapshot)
                                  ->sequence_number()
                            : latest_snapshot), seed); // 返回DBIter类型的迭代器
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    MaybeScheduleCompaction();
  }
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  MutexLock l(&mutex_);
  snapshots_.Delete(static_cast<const SnapshotImpl*>(snapshot));
}

// 在执行db->Put()时，先调用DBImpl::Put()
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);  // 调用DBImpl::Write()
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
  Writer w(&mutex_);    // 构建一个写请求，不是加锁
  w.batch = updates;
  w.sync = options.sync;    // 默认false
  w.done = false;

  // 1，队长统一提交，其他调用者等待
  // 2，队长写数期间放开了锁，其他调用者可以继续排队
  // 3，队长写数结束后，一个是通知排队者，一个是通知下一个队长继续
  MutexLock l(&mutex_); // 加锁，对所有写线程来说都是唯一的一份，位于util/mutexlock.h
  writers_.push_back(&w);   // deque
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();    // 释放上面刚刚加锁的mutex_，释放是为了让后续的写进程能够进入队列
  }
  if (w.done) {
    return w.status;
  }
  // 整个Writer函数，只有这里涉及合并操作
  Status status = MakeRoomForWrite(updates == nullptr); // 会放开锁一段时间，此时也可能有新的写线程进来
  uint64_t last_sequence = versions_->LastSequence();
  printf("last_sequence=%llu\n",last_sequence);
  Writer* last_writer = &w;
  if (status.ok() && updates != nullptr) {  // nullptr batch is for compactions
    WriteBatch* write_batch = BuildBatchGroup(&last_writer);    // 合并队列中的多个WriteBatch
    WriteBatchInternal::SetSequence(write_batch, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(write_batch);

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    {
      mutex_.Unlock();  // 队长准备好了memtable空间，后来者均会排队，此处放锁并IO
      status = log_->AddRecord(WriteBatchInternal::Contents(write_batch));  // 写入WAL
      bool sync_error = false;
      if (status.ok() && options.sync) {
        status = logfile_->Sync();
        if (!status.ok()) {
          sync_error = true;
        }
      }
      if (status.ok()) {
        status = WriteBatchInternal::InsertInto(write_batch, mem_); // 将批处理应用到 memtable
      }
      mutex_.Lock();
      if (sync_error) {
        // The state of the log file is indeterminate: the log record we
        // just added may or may not show up when the DB is re-opened.
        // So we force the DB into a mode where all future writes fail.
        RecordBackgroundError(status);
      }
    }
    if (write_batch == tmp_batch_) tmp_batch_->Clear();

    versions_->SetLastSequence(last_sequence);
  }

  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;    // last_writer指向本次批量提交的最后一个队员
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();  // 后面又解锁了一段时间，所以可能有新的写线程进来，这里是唤醒下一批的队长
  }

  return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-null batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  mutex_.AssertHeld();  // 断言，判断当前线程是否持有 mutex_ 锁，持有锁才会继续往下走
  assert(!writers_.empty());    // 断言，确保 writers_ 队列非空
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != nullptr);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128 << 10)) {
    max_size = size + (128 << 10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != nullptr) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}


Status DBImpl::MakeRoomForWrite(bool force) {   // force = (updates == nullptr)，正常情况为false
  mutex_.AssertHeld();  // 断言，判断当前线程是否持有 mutex_ 锁，持有锁才会继续往下走
  assert(!writers_.empty());    // 断言，确保 writers_ 队列非空
  bool allow_delay = !force;    // true
  Status s;
  while (true) {
    if (!bg_error_.ok()) {
      // Yield previous error
      s = bg_error_;
      break;
    } else if (allow_delay && versions_->NumLevelFiles(0) >=
                                  config::kL0_SlowdownWritesTrigger) {  // 8，如果level 0层的SST的数量超过8
      // We are getting close to hitting a hard limit on the number of
      // L0 files.  Rather than delaying a single write by several
      // seconds when we hit the hard limit, start delaying each
      // individual write by 1ms to reduce latency variance.  Also,
      // this delay hands over some CPU to the compaction thread in
      // case it is sharing the same core as the writer.
      mutex_.Unlock();
      env_->SleepForMicroseconds(1000); // 当前获得锁的写线程延迟1毫秒，while循环在走一遍if-else
      allow_delay = false;  // Do not delay a single write more than once
      mutex_.Lock();
    } else if (!force &&
               (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
      // 表示当前 memtable 有足够空间，无需更多操作，退出循环
      break;
    } else if (imm_ != nullptr) {
      // We have filled up the current memtable, but the previous
      // one is still being compacted, so we wait.
      Log(options_.info_log, "Current memtable full; waiting...\n");
      background_work_finished_signal_.Wait();  // 等待后台工作（主要是 imm_ 的 compaction）完成的信号
    } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {  // 12，如果level 0层的SST的数量超过12
      // There are too many level-0 files.
      Log(options_.info_log, "Too many L0 files; waiting...\n");
      background_work_finished_signal_.Wait();  // 等待后台 compaction 完成，以减少 L0 文件的数量
    } else {
      // 1. 当前 mem_ 已满 (或 force 为 true)
      // 2. 没有 imm_ 正在处理 (imm_ == nullptr)
      // 3. L0 SST文件数量未达到停止写入的硬限制 (SST数量小于8)
      assert(versions_->PrevLogNumber() == 0);
      uint64_t new_log_number = versions_->NewFileNumber(); // 为新的 WAL 日志文件分配一个编号
      WritableFile* lfile = nullptr;
      s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);  // 创建新的 WAL 日志文件
      if (!s.ok()) {
        // Avoid chewing through file number space in a tight loop.
        versions_->ReuseFileNumber(new_log_number);
        break;
      }

      delete log_;  // 销毁旧的 log::Writer 对象

      s = logfile_->Close();  // 关闭底层的 WritableFile 文件句柄
      if (!s.ok()) {
        // We may have lost some data written to the previous log file.
        // Switch to the new log file anyway, but record as a background
        // error so we do not attempt any more writes.
        //
        // We could perhaps attempt to save the memtable corresponding
        // to log file and suppress the error if that works, but that
        // would add more complexity in a critical code path.
        RecordBackgroundError(s);
      }
      delete logfile_;  // 销毁底层的 WritableFile 对象，释放资源

      // 生成新的数据库实例的日志文件和写入器指针
      logfile_ = lfile;
      logfile_number_ = new_log_number;
      log_ = new log::Writer(lfile);
      imm_ = mem_;
      has_imm_.store(true, std::memory_order_release);  // 标记存在 imm_
      mem_ = new MemTable(internal_comparator_);    // 创建一个新的可写 memtable (mem_)
      mem_->Ref();
      force = false;  // Do not force another compaction if have room
      Log(options_.info_log, "Memtable switched to immutable; imm_ size: %llu bytes", static_cast<unsigned long long>(imm_->ApproximateMemoryUsage()));
      MaybeScheduleCompaction();    // 调度一次 compaction 检查，因为现在有了新的 imm_ 需要处理
    }
  }
  return s;
}
// 接收一个 property 参数（表示要查询的属性名称），value 指针（用于存储查询结果）
// 比如 property 为 "leveldb.num-files-at-level0" 时，表示查询 level 0 的文件数量
bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      std::snprintf(buf, sizeof(buf), "%d",
                    versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    char buf[200];
    std::snprintf(buf, sizeof(buf),
                  "                               Compactions\n"
                  "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
                  "--------------------------------------------------\n");
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        std::snprintf(buf, sizeof(buf), "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
                      level, files, versions_->NumLevelBytes(level) / 1048576.0,
                      stats_[level].micros / 1e6,
                      stats_[level].bytes_read / 1048576.0,
                      stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  } else if (in == "approximate-memory-usage") {
    size_t total_usage = options_.block_cache->TotalCharge();
    if (mem_) {
      total_usage += mem_->ApproximateMemoryUsage();
    }
    if (imm_) {
      total_usage += imm_->ApproximateMemoryUsage();
    }
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%llu",
                  static_cast<unsigned long long>(total_usage));
    value->append(buf);
    return true;
  }

  return false;
}
// 输入range数组指针和数组大小n，sizes是输出数组
// 估算 LevelDB 数据库中指定范围（Range）内的数据大小（以字节为单位）。
void DBImpl::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
  // TODO(opt): better implementation
  MutexLock l(&mutex_);
  Version* v = versions_->current();
  v->Ref();

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1); // start是一个累加值
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  v->Unref();
}

Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) { // 注意，没有语法错误，虽然DB是抽象类，不能直接实例化
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);    // 调用DBImpl::Write
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() = default;

Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {    // include/leveldb/db.h中，只有一个Open，且是static函数
  *dbptr = nullptr; // 初始化数据库指针为空，表示尚未成功打开

  DBImpl* impl = new DBImpl(options, dbname); // DBImpl是DB的实现类
  impl->mutex_.Lock();  // TODO
  VersionEdit edit; // 用于记录版本变更信息，这些变更将应用于 MANIFEST 文件
  // Recover handles create_if_missing, error_if_exists
  bool save_manifest = false;
  // 1. 基于 MANIFEST 文件恢复 VersionSet（数据库元数据快照）
  // 2. 重放 WAL (Write-Ahead Log) 日志文件，将未持久化到 SSTable 的数据恢复到 MemTable
  // 3. 更新序列号 (sequence number) 和文件编号 (file number)
  // 4. 生成描述这些变更的 VersionEdit
  Status s = impl->Recover(&edit, &save_manifest);
  if (s.ok() && impl->mem_ == nullptr) {  // 这通常发生在数据库是全新创建的，或者没有WAL文件重放为MemTable
    // Create new log and a corresponding memtable.
    uint64_t new_log_number = impl->versions_->NewFileNumber(); // 创建WAL的新编号
    WritableFile* lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number),
                                     &lfile); // 创建新的日志文件
    if (s.ok()) {
      edit.SetLogNumber(new_log_number);  // 在 VersionEdit 中记录新的当前日志文件编号
      impl->logfile_ = lfile; // DBImpl 持有该日志文件的句柄
      impl->logfile_number_ = new_log_number;
      impl->log_ = new log::Writer(lfile);  // 创建一个日志写入器，用于向新的日志文件写入记录
      impl->mem_ = new MemTable(impl->internal_comparator_);
      impl->mem_->Ref();
    }
  }
  if (s.ok() && save_manifest) {  // save_manifest标志着恢复过程中是否产生了需要持久化的元数据变更。
    edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
    edit.SetLogNumber(impl->logfile_number_); 
    s = impl->versions_->LogAndApply(&edit, &impl->mutex_); // 将累积的元数据变更 (edit) 应用到 VersionSet，并持久化到新的 MANIFEST 文件，生成新的 Version
  }
  // recovery恢复memtable并compact sst后，再尝试做一次sst之间的compaction
  if (s.ok()) {
    impl->RemoveObsoleteFiles();  // 清理掉不再需要的旧文件（例如，旧的日志文件、已被合并的 SSTable 文件）
    impl->MaybeScheduleCompaction();
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    assert(impl->mem_ != nullptr);
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Snapshot::~Snapshot() = default;    // Snapshot类声明在 include/leveldb/db.h中

Status DestroyDB(const std::string& dbname, const Options& options) {   // 声明在include/leveldb/db.h中
  Env* env = options.env;
  std::vector<std::string> filenames;
  Status result = env->GetChildren(dbname, &filenames); // 不是递归找，只找第一层的文件和文件夹
  if (!result.ok()) {
    // Ignore error in case directory does not exist
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->RemoveFile(dbname + "/" + filenames[i]);  // 貌似只删除文件，不删除非空目录
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->RemoveFile(lockname);
    env->RemoveDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}  // namespace leveldb
