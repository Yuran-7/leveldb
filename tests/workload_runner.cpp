// LevelDB workload runner adapted from a RocksDB-style driver.
// Supports operations:
//  I key value   -> Insert/Put
//  U key value   -> Update/Put
//  D key         -> Delete
//  R start end   -> Range delete [start, end)
//  Q key         -> Point query
//  S start end   -> Range scan [start, end) existence check

#include <chrono>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <memory>

#include "leveldb/db.h"
#include "leveldb/iterator.h"
#include "leveldb/options.h"
#include "leveldb/status.h"
#include "leveldb/write_batch.h"

namespace {

bool ParseLine(const std::string& line, char& op, std::vector<std::string>& tokens) {
    tokens.clear();
    if (line.empty()) return false;
    if (line[0] == '#') return false;

    std::istringstream iss(line);
    if (!(iss >> op)) return false;

    switch (op) {
        case 'I':
        case 'U': {
            std::string key;
            if (!(iss >> key)) return false;
            std::string value;
            // Capture the rest of the line as value (may contain spaces)
            std::getline(iss, value);
            if (!value.empty() && value[0] == ' ') value.erase(0, 1);
            if (value.empty()) return false;
            tokens.push_back(std::move(key));
            tokens.push_back(std::move(value));
            return true;
        }
        case 'D':
        case 'Q': {
            std::string key;
            if (!(iss >> key)) return false;
            tokens.push_back(std::move(key));
            return true;
        }
        case 'R':
        case 'S': {
            std::string start_key, end_key;
            if (!(iss >> start_key >> end_key)) return false;
            tokens.push_back(std::move(start_key));
            tokens.push_back(std::move(end_key));
            return true;
        }
        default:
            return false;
    }
}

// Range delete [start_key, end_key) by iterating keys and batching deletes.
leveldb::Status RangeDelete(leveldb::DB* db, const std::string& start_key,
                                                        const std::string& end_key, size_t batch_limit = 1000) {
    leveldb::ReadOptions ro;
    // Use a snapshot so iteration is not affected by our deletes.
    const leveldb::Snapshot* snap = db->GetSnapshot();
    ro.snapshot = snap;
    std::unique_ptr<leveldb::Iterator> it(db->NewIterator(ro));

    leveldb::WriteBatch batch;
    size_t in_batch = 0;
    leveldb::Status last_status;
    for (it->Seek(start_key); it->Valid(); it->Next()) {
        const std::string k = it->key().ToString();
        if (k >= end_key) break;
        batch.Delete(k);
        in_batch++;
        if (in_batch >= batch_limit) {
            last_status = db->Write(leveldb::WriteOptions(), &batch);
            batch.Clear();
            in_batch = 0;
            if (!last_status.ok()) break;
        }
    }
    if (last_status.ok() && in_batch > 0) {
        last_status = db->Write(leveldb::WriteOptions(), &batch);
    }
    db->ReleaseSnapshot(snap);
    if (!it->status().ok() && last_status.ok()) return it->status();
    return last_status;
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <database_path> <workload_file_path>" << std::endl;
        return 1;
    }

    const std::string db_path = argv[1];
    const std::string workload_file_path = argv[2];

    leveldb::Options options;
    options.create_if_missing = true;
    // Optional tuning similar to the RocksDB example
    options.write_buffer_size = 64 * 1024 * 1024; // 64MB
    options.max_file_size = 64 * 1024 * 1024;     // 64MB target sst size

    leveldb::DB* db = nullptr;
    leveldb::Status s = leveldb::DB::Open(options, db_path, &db);
    if (!s.ok()) {
        std::cerr << "Error opening database " << db_path << ": " << s.ToString() << std::endl;
        return 1;
    }
    std::cout << "Database opened successfully: " << db_path << std::endl;

    std::ifstream fin(workload_file_path);
    if (!fin.is_open()) {
        std::cerr << "Error opening workload file: " << workload_file_path << std::endl;
        delete db;
        return 1;
    }
    std::cout << "Processing workload file: " << workload_file_path << std::endl;

    // Metrics
    long long line_number = 0;
    long long operations_processed = 0;
    long long insert_count = 0;
    long long update_count = 0;
    long long delete_count = 0;
    long long range_delete_count = 0;
    long long point_query_found_count = 0;
    long long point_query_not_found_count = 0;
    long long range_query_found_count = 0;
    long long range_query_not_found_count = 0;
    long long total_data_size_bytes = 0;  // Sum of key+value for inserts/updates

    const auto start_time = std::chrono::high_resolution_clock::now();

    std::string line;
    while (std::getline(fin, line)) {
        line_number++;
        char op;
        std::vector<std::string> tokens;
        if (!ParseLine(line, op, tokens)) {
            if (!line.empty() && line[0] != '#') {
                std::cerr << "Skipping malformed or invalid line " << line_number << ": \"" << line << "\"" << std::endl;
            }
            continue;
        }

        operations_processed++;
        switch (op) {
            case 'I': {
                const std::string& key = tokens[0];
                const std::string& value = tokens[1];
                s = db->Put(leveldb::WriteOptions(), key, value);
                if (!s.ok()) {
                    std::cerr << "Line " << line_number << ": Put failed for key '" << key << "': " << s.ToString() << std::endl;
                } else {
                    insert_count++;
                    total_data_size_bytes += static_cast<long long>(key.size() + value.size());
                }
                break;
            }
            case 'U': {
                const std::string& key = tokens[0];
                const std::string& value = tokens[1];
                s = db->Put(leveldb::WriteOptions(), key, value);
                if (!s.ok()) {
                    std::cerr << "Line " << line_number << ": Update failed for key '" << key << "': " << s.ToString() << std::endl;
                } else {
                    update_count++;
                    total_data_size_bytes += static_cast<long long>(key.size() + value.size());
                }
                break;
            }
            case 'D': {
                const std::string& key = tokens[0];
                s = db->Delete(leveldb::WriteOptions(), key);
                if (!s.ok()) {
                    std::cerr << "Line " << line_number << ": Delete failed for key '" << key << "': " << s.ToString() << std::endl;
                } else {
                    delete_count++;
                }
                break;
            }
            case 'R': {
                const std::string& start_key = tokens[0];
                const std::string& end_key = tokens[1];
                s = RangeDelete(db, start_key, end_key);
                if (!s.ok()) {
                    std::cerr << "Line " << line_number << ": RangeDelete failed: " << s.ToString() << std::endl;
                } else {
                    range_delete_count++;
                }
                break;
            }
            case 'Q': {
                const std::string& key = tokens[0];
                std::string value;
                s = db->Get(leveldb::ReadOptions(), key, &value);
                if (s.ok()) {
                    point_query_found_count++;
                } else if (s.IsNotFound()) {
                    point_query_not_found_count++;
                } else {
                    std::cerr << "Line " << line_number << ": Get failed for key '" << key << "': " << s.ToString() << std::endl;
                }
                break;
            }
            case 'S': {
                const std::string& start_key = tokens[0];
                const std::string& end_key = tokens[1];
                leveldb::ReadOptions ro;
                std::unique_ptr<leveldb::Iterator> it(db->NewIterator(ro));
                bool found_any = false;
                for (it->Seek(start_key); it->Valid(); it->Next()) {
                    if (it->key().ToString() >= end_key) break;
                    found_any = true;
                    break;  // We only care if at least one key exists in the range.
                }
                if (!it->status().ok()) {
                    std::cerr << "Line " << line_number << ": Iterator error: " << it->status().ToString() << std::endl;
                } else if (found_any) {
                    range_query_found_count++;
                } else {
                    range_query_not_found_count++;
                }
                break;
            }
            default:
                std::cerr << "ERROR: Unknown op '" << op << "'" << std::endl;
                break;
        }

        if (operations_processed % 100000 == 0) {
            std::cout << "Processed " << operations_processed << " operations" << std::endl;
        }
    }

    const auto end_time = std::chrono::high_resolution_clock::now();
    const double duration_s = std::chrono::duration<double>(end_time - start_time).count();
    const double ops_per_sec = duration_s > 0.0 ? operations_processed / duration_s : 0.0;
    const double throughput_mb_s = duration_s > 0.0
                                                                         ? (static_cast<double>(total_data_size_bytes) / (1024.0 * 1024.0)) / duration_s
                                                                         : 0.0;

    std::cout << "Finished processing workload file." << std::endl;
    std::cout << "Total lines read: " << line_number << std::endl;
    std::cout << "Total valid operations processed: " << operations_processed << std::endl;
    std::cout << "Total inserts: " << insert_count << std::endl;
    std::cout << "Total updates: " << update_count << std::endl;
    std::cout << "Total deletes: " << delete_count << std::endl;
    std::cout << "Total range deletes: " << range_delete_count << std::endl;
    std::cout << "Total found point queries: " << point_query_found_count << std::endl;
    std::cout << "Total not-found point queries: " << point_query_not_found_count << std::endl;
    std::cout << "Total found range queries: " << range_query_found_count << std::endl;
    std::cout << "Total not-found range queries: " << range_query_not_found_count << std::endl;
    std::cout << "Total data written (I/U key+value): "
                        << (static_cast<double>(total_data_size_bytes) / (1024.0 * 1024.0)) << " MB" << std::endl;
    std::cout << "Execution time: " << duration_s << " s" << std::endl;
    std::cout << "Operations per second (OPS): " << ops_per_sec << std::endl;
    std::cout << "Throughput (MB/s): " << throughput_mb_s << std::endl;

    fin.close();
    delete db;
    std::cout << "Database closed." << std::endl;

    return 0;
}

/*
g++ -std=c++17 -O2 \
  -I/NV1/ysh/leveldb/include \
  -I/NV1/ysh/leveldb/build/include \
  /NV1/ysh/leveldb/tests/workload_runner.cpp \
  /NV1/ysh/leveldb/build/libleveldb.a \
  -o /NV1/ysh/leveldb/tests/workload_runner \
  -pthread -ldl -lsnappy -lzstd
*/

// ./workload_runner ./tests/testdb /NV1/ysh/dataset/workload.txt