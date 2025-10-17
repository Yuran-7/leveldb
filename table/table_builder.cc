// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

struct TableBuilder::Rep {
  // 构造函数，初始化TableBuilder的状态
  Rep(const Options& opt, WritableFile* f)
      : options(opt),                               // 保存传入的选项
        index_block_options(opt),                   // 索引块使用和数据块一样的选项
        file(f),                                    // 要写入的SSTable文件
        offset(0),                                  // 当前文件偏移量
        data_block(&options),                       // 当前正在构建的数据块
        index_block(&index_block_options),          // 当前正在构建的索引块
        num_entries(0),                             // 已添加的条目总数
        closed(false),                              // TableBuilder是否已关闭 (调用了Finish或Abandon)
        filter_block(opt.filter_policy == nullptr  // 如果设置了过滤器策略，则创建过滤器块构建器
                         ? nullptr
                         : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {  // 是否有一个待处理的索引条目
    // 索引块的重启点间隔设置为1，意味着每个条目都是一个重启点。
    // 这使得在索引块中进行二分查找时，不需要线性扫描来找到目标条目。
    index_block_options.block_restart_interval = 1;
  }

  Options options;                  // Table的选项
  Options index_block_options;      // 索引块的选项
  WritableFile* file;               // SSTable文件
  uint64_t offset;                  // 当前写入文件的偏移量
  Status status;                    // 当前的状态，如果不是ok()，则停止后续操作
  BlockBuilder data_block;          // 数据块构建器
  BlockBuilder index_block;         // rocksdb中这个是std::unique_ptr<IndexBuilder>
  std::string last_key;             // 最近添加的key
  int64_t num_entries;              // 已添加的条目总数
  bool closed;                      // 要么 Finish() 要么 Abandon() 已被调用。
  FilterBlockBuilder* filter_block; // 过滤器块构建器，rocksdb中std::unique_ptr<FilterBlockBuilder>

  // 我们不会在看到下一个数据块的第一个键之前，为当前块生成索引条目。
  // 这允许我们在索引块中使用更短的键。例如，考虑一个块边界
  // 在 "the quick brown fox" 和 "the who" 这两个键之间。我们可以使用
  // "the r" 作为索引块条目的键，因为它 >= 第一个块中的所有条目
  // 并且 < 后续块中的所有条目。
  //
  // 不变性: r->pending_index_entry 为 true 当且仅当 data_block 为空。
  bool pending_index_entry;
  BlockHandle pending_handle;  // 待添加到索引块的句柄

  std::string compressed_output;  // 压缩后的数据块的临时存储
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  // 如果定义了过滤器，则开始构建第一个过滤器块
  if (rep_->filter_block != nullptr) {
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  // 捕获调用者忘记调用 Finish() 或 Abandon() 的错误
  assert(rep_->closed);
  delete rep_->filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // 注意: 如果向 Options 添加更多字段，请更新此函数
  // 以捕获在构建 Table 过程中不应更改的更改。
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // 注意，任何活动的 BlockBuilder 都指向 rep_->options，因此
  // 将自动获取更新后的选项。
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}

void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed); // 确认 TableBuilder 未关闭
  if (!ok()) return; // 如果状态不正常，则直接返回
  if (r->num_entries > 0) {
    // 确认新添加的 key 大于上一个 key
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }

  // 如果有一个待处理的索引条目，现在就处理它
  if (r->pending_index_entry) {
    assert(r->data_block.empty());
    // 找到一个最短的分隔符，用于上一个块的索引
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    std::string handle_encoding;
    r->pending_handle.EncodeTo(&handle_encoding);
    // 将上一个块的索引条目（分隔符键和块句柄）添加到索引块
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    r->pending_index_entry = false;
  }

  // 如果有过滤器，则将 key 添加到过滤器
  if (r->filter_block != nullptr) {
    r->filter_block->AddKey(key);
  }

  // 更新 last_key，增加条目计数，并将键值对添加到数据块
  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  r->data_block.Add(key, value);

  // 如果当前数据块的大小估算值超过了选项中设置的块大小，则刷写数据块
  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}

// 将当前数据块写入文件
void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  // 将数据块写入文件，并获取其句柄
  WriteBlock(&r->data_block, &r->pending_handle);
  if (ok()) {
    // 标记有一个待处理的索引条目
    r->pending_index_entry = true;
    // 刷写文件缓冲区
    r->status = r->file->Flush(); // 写入操作系统内核缓冲区，数据还在内存中，但在内核空间
  }
  // 如果有过滤器，则为新的数据块开始一个新的过滤器块
  if (r->filter_block != nullptr) {
    r->filter_block->StartBlock(r->offset);
  }
}

void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // 文件格式包含一系列块，每个块具有：
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  // 完成块的构建，获取原始数据
  Slice raw = block->Finish();

  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): 支持更多压缩选项: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      // 尝试使用 Snappy 压缩，并且压缩后的大小要比原始大小至少小 12.5%
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // 如果 Snappy 不支持，或者压缩效果不佳，则直接存储未压缩的形式
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }

    case kZstdCompression: {
      std::string* compressed = &r->compressed_output;
      // 尝试使用 Zstd 压缩，并且压缩后的大小要比原始大小至少小 12.5%
      if (port::Zstd_Compress(r->options.zstd_compression_level, raw.data(),
                              raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // 如果 Zstd 不支持，或者压缩效果不佳，则直接存储未压缩的形式
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  // 将处理后（可能已压缩）的块内容写入文件
  WriteRawBlock(block_contents, type, handle);
  // 清空临时压缩输出字符串并重置块构建器
  r->compressed_output.clear();
  block->Reset();
}

void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type, BlockHandle* handle) {
  Rep* r = rep_;
  // 设置块句柄的偏移量和大小
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());
  // 将块内容追加到文件
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    // 写入块的尾部信息（压缩类型和CRC校验和）
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // 将CRC扩展以覆盖块类型
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    // 将尾部信息追加到文件
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      // 更新文件偏移量
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status TableBuilder::status() const { return rep_->status; }

// 完成Table的构建
Status TableBuilder::Finish() {
  Rep* r = rep_;
  // 刷写最后一个数据块（如果存在）
  Flush();
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // 写入过滤器块
  if (ok() && r->filter_block != nullptr) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }

  // 写入元数据索引块 (metaindex block)，这个元数据块只有一个键值对，key是filter.default，value是过滤器数据的位置
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != nullptr) {
      // 添加从 "filter.Name" 到过滤器数据位置的映射
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): 添加统计信息和其他元数据块
    WriteBlock(&meta_index_block, &metaindex_block_handle); // 带去压缩
  }

  // 写入索引块
  if (ok()) {
    if (r->pending_index_entry) {
      // 为最后一个数据块生成一个最短的后继键作为索引
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // 写入文件尾部 (Footer)
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

// 放弃构建，将TableBuilder标记为关闭
void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

// 返回已添加的条目数
uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

// 返回当前文件的大小
uint64_t TableBuilder::FileSize() const { return rep_->offset; }

}  // namespace leveldb
