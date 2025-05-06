#pragma once

#include "rocksdb/db.h"

namespace ROCKSDB_NAMESPACE 
{
  struct ElasticLSMOptions
  {
    int max_background_threads = 32;
    int min_tp_threads = 2;
    int min_ap_threads = 2;
    int min_compaction_threads = 2;
  };
  class ElasticLSM
  {
  public:
    ElasticLSM() {};
    ElasticLSM(const ElasticLSM&) = delete;
    void operator=(const ElasticLSM&) = delete;

    virtual ~ElasticLSM();

    static Status Open(const Options& options, const std::string& name,
      std::unique_ptr<DB>* dbptr);

    virtual Status Put(const WriteOptions& options, ColumnFamilyHandle* column_family,
      const Slice& key, const Slice& value);
    Status Put(const WriteOptions& options, const Slice& key, const Slice& value) {
      return Put(options, DefaultColumnFamily(), key, value);
    }

    virtual Status Delete(const WriteOptions& options,
      ColumnFamilyHandle* column_family,
      const Slice& key);
    Status Delete(const WriteOptions& options, const Slice& key) {
      return Delete(options, DefaultColumnFamily(), key);
    }

    virtual Status Update(const WriteOptions& options, ColumnFamilyHandle* column_family,
      const Slice& key, const Slice& value);
    Status Update(const WriteOptions& options,
      const Slice& key, const Slice& value) {
      return Update(options, DefaultColumnFamily(), key, value);
    }

    virtual Status Get(const ReadOptions& _read_options,
      ColumnFamilyHandle* column_family, const Slice& key,
      std::string* value);
    Status Get(const ReadOptions& _read_options, const Slice& key,
      std::string* value) {
      return Get(_read_options, DefaultColumnFamily(), key, value);
    }

    virtual Status Scan(const ReadOptions& _read_options,
      ColumnFamilyHandle* column_family, const Slice &key, int record_count,
      const std::function<void(PinnableSlice*)>& func);
    Status Scan(const ReadOptions& _read_options, const Slice &key, int record_count,
      const std::function<void(PinnableSlice*)>& func) {
      return Scan(_read_options, DefaultColumnFamily(), key, record_count, func);
    }

    virtual ColumnFamilyHandle* DefaultColumnFamily() const = 0;
  private:
    ElasticLSMOptions options_;
  };
}