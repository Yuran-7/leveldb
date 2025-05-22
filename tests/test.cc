#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#include "leveldb/comparator.h"
#include <string>
#include <iostream>
using namespace std;
using namespace leveldb;
//g++ -g -o test test.cc ../build/libleveldb.a -I../include -pthread -lsnappy
int main(){
    // DB 对象是线程安全的。多个线程可以安全地同时使用同一个 DB 实例调用 Put、Delete、Get 和 Write
    DB* db;
    // Open
    Options options;
    options.create_if_missing=true; // 如果数据库目录不存在，则自动创建一个新的 LevelDB 数据库
    // options.filter_policy = leveldb::NewBloomFilterPolicy(10);
    string name="testdb";
    Status status=DB::Open(options,name,&db);
    cout<<status.ToString()<<endl;

    // Put
    WriteOptions woptions;
    status=db->Put(woptions,"name","owenliang");

    // 手动触发 MemTable -> SST 文件的压缩操作，在version_set.cc中实现
    // db->CompactRange(nullptr, nullptr);

    // if(options.comparator==BytewiseComparator()){
    //     cout<<"BytewiseComparator"<<endl;
    // }

    // Get
    ReadOptions roptions;
    string value;
    status=db->Get(roptions,"name",&value);
    cout<<status.ToString()<<","<<value<<endl;
    // WriteBatch
    WriteBatch batch;
    batch.Put("a","1");
    batch.Put("b","2");
    status=db->Write(woptions,&batch);
    // Delete
    db->Delete(woptions,"name");
    // Iterator
    Iterator *iter=db->NewIterator(roptions);
    iter->SeekToFirst();
    while(iter->Valid()){
        Slice key=iter->key();
        Slice value=iter->value();
        cout<<key.ToString()<<"="<<value.ToString()<<endl;
        iter->Next();
    }
    delete iter;
    delete db;
    return 0;
}