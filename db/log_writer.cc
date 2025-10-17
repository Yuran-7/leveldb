// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <cstdint>

#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

// 初始化记录类型的 CRC 校验值数组
// 为每种记录类型（kFullType, kFirstType, kMiddleType, kLastType）预计算 CRC 值
// 这样在写入记录时可以直接使用，避免重复计算
static void InitTypeCrc(uint32_t* type_crc) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc[i] = crc32c::Value(&t, 1);  // 计算单个字节（记录类型）的 CRC 值
  }
}

// 构造函数：用于新建的日志文件
// dest: 目标文件的可写句柄
// block_offset_: 初始化为 0，表示从文件开头开始写入
Writer::Writer(WritableFile* dest) : dest_(dest), block_offset_(0) {
  InitTypeCrc(type_crc_);  // 初始化 CRC 校验表
}

// 构造函数：用于追加到已存在的日志文件
// dest_length: 已存在文件的长度
// block_offset_: 设置为 dest_length % kBlockSize，表示在当前块中的偏移量
// 这样可以继续在当前块中写入，而不是从新块开始
Writer::Writer(WritableFile* dest, uint64_t dest_length)
    : dest_(dest), block_offset_(dest_length % kBlockSize) {
  InitTypeCrc(type_crc_);
}

Writer::~Writer() = default;

// 添加一条记录到日志文件
// slice: 要写入的数据
// 
// WAL 日志文件组织结构：
// - 日志文件被分成固定大小的块（kBlockSize = 32KB）
// - 每条记录包含：header(7字节) + data
// - 如果记录太大，会被分片存储到多个块中
Status Writer::AddRecord(const Slice& slice) {
  const char* ptr = slice.data();
  size_t left = slice.size();  // 剩余未写入的字节数

  // 如果需要，将记录分片并写入
  // 注意：即使 slice 为空，我们仍然要迭代一次来写入一个长度为零的记录
  Status s;
  bool begin = true;  // 标记是否是记录的开始部分
  do {
    const int leftover = kBlockSize - block_offset_;  // 当前块中剩余的空间
    assert(leftover >= 0);
    
    if (leftover < kHeaderSize) {  // 如果剩余空间不足以写入一个完整的 header（7 字节）
      // 切换到新块
      if (leftover > 0) {
        // 用 0 填充当前块的尾部（trailer）
        // 下面的字面量依赖于 kHeaderSize 为 7
        static_assert(kHeaderSize == 7, "");
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
      }
      block_offset_ = 0;  // 重置为新块的起始位置
    }

    // 不变量：我们永远不会在一个块中留下少于 kHeaderSize 字节的空间
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;  // 当前块可用于写入数据的空间
    const size_t fragment_length = (left < avail) ? left : avail;  // 本次写入的片段长度

    // 确定记录类型
    RecordType type;
    const bool end = (left == fragment_length);  // 判断是否是记录的最后一个片段
    if (begin && end) {
      type = kFullType;      // 完整记录（一个块就能装下）
    } else if (begin) {
      type = kFirstType;     // 记录的第一个片段
    } else if (end) {
      type = kLastType;      // 记录的最后一个片段
    } else {
      type = kMiddleType;    // 记录的中间片段
    }

    s = EmitPhysicalRecord(type, ptr, fragment_length);  // 写入物理记录
    ptr += fragment_length;   // 移动数据指针
    left -= fragment_length;  // 减少剩余字节数
    begin = false;            // 后续片段不再是开始部分
  } while (s.ok() && left > 0);  // 继续直到所有数据都写完或发生错误
  return s;
}

// 写入一条物理记录到日志文件
// t: 记录类型（kFullType, kFirstType, kMiddleType, kLastType）
// ptr: 数据指针
// length: 数据长度
//
// 记录格式（header 7 字节 + data）：
// +-----------+-----------+-----------+--- ... ---+
// | CRC (4B)  | Size (2B) | Type (1B) | Payload   |
// +-----------+-----------+-----------+--- ... ---+
Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr,
                                  size_t length) {
  assert(length <= 0xffff);  // 长度必须能用两个字节表示（最大 65535 字节）
  assert(block_offset_ + kHeaderSize + length <= kBlockSize);  // 确保不超过块大小

  // 格式化 header（7 字节）
  char buf[kHeaderSize];
  buf[4] = static_cast<char>(length & 0xff);        // 长度低字节
  buf[5] = static_cast<char>(length >> 8);          // 长度高字节
  buf[6] = static_cast<char>(t);                    // 记录类型

  // 计算记录类型和数据的 CRC 校验和
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, length);  // 基于预计算的类型 CRC 扩展计算
  crc = crc32c::Mask(crc);  // 调整 CRC 值以便存储（防止全 0）
  EncodeFixed32(buf, crc);  // 将 CRC 编码到 buf 的前 4 个字节

  // 写入 header 和数据
  Status s = dest_->Append(Slice(buf, kHeaderSize));  // 写入 7 字节的 header
  if (s.ok()) {
    s = dest_->Append(Slice(ptr, length));  // 写入实际数据
    if (s.ok()) {
      s = dest_->Flush();  // 刷新到文件系统缓冲区
    }
  }
  block_offset_ += kHeaderSize + length;  // 更新当前块中的偏移量
  return s;
}

}  // namespace log
}  // namespace leveldb
