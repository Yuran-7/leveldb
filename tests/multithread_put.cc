#include "leveldb/db.h"
#include <thread>
#include <vector>
#include <iostream>
#include <sstream>

using namespace std;
using namespace leveldb;

void thread_put(DB* db, int thread_id, int num_kv) {
    WriteOptions write_options;
    for (int i = 0; i < num_kv; ++i) {
        stringstream key, value;
        key << "key_" << thread_id << "_" << i;
        value << "value_" << thread_id << "_" << i;

        Status status = db->Put(write_options, key.str(), value.str());
        if (!status.ok()) {
            cerr << "Thread " << thread_id << " failed to put: " << status.ToString() << endl;
        }
    }
}

int main() {
    DB* db;
    Options options;
    options.create_if_missing = true;
    Status status = DB::Open(options, "testdb", &db);
    if (!status.ok()) {
        cerr << "Failed to open DB: " << status.ToString() << endl;
        return 1;
    }

    const int num_threads = 4;
    const int kv_per_thread = 1000;

    vector<thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(thread_put, db, i, kv_per_thread);
    }

    for (auto& t : threads) {
        t.join();
    }

    // 👇 添加这一行：强制刷 MemTable -> SST 文件
    // db->CompactRange(nullptr, nullptr);
    cout << "All threads finished writing." << endl;

    delete db;
    return 0;
}

// g++ -std=c++11 -pthread -o multithread_put multithread_put.cc ../build/libleveldb.a -I../include -lsnappy