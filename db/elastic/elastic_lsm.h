#pragma once

#include "db/elastic/db_elastic.h"
#include "db/elastic/task.h"
#include "util/threadpool_imp.h"

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
    ElasticLSM();
    static Status Open(const Options& options, const std::string& name,
      std::unique_ptr<DB>* dbptr);

    Status Put(const WriteOptions& options, ColumnFamilyHandle* column_family,
      const Slice& key, const Slice& value);
    Status Put(const WriteOptions& options, const Slice& key, const Slice& value) {
      return Put(options, db_->DefaultColumnFamily(), key, value);
    }

    Status Delete(const WriteOptions& options,
      ColumnFamilyHandle* column_family,
      const Slice& key);
    Status Delete(const WriteOptions& options, const Slice& key) {
      return Delete(options, db_->DefaultColumnFamily(), key);
    }

    Status Update(const WriteOptions& options, ColumnFamilyHandle* column_family,
      const Slice& key, const Slice& value);
    Status Update(const WriteOptions& options,
      const Slice& key, const Slice& value) {
      return Update(options, db_->DefaultColumnFamily(), key, value);
    }

    Status Get(const ReadOptions& _read_options,
      ColumnFamilyHandle* column_family, const Slice& key,
      std::string* value);
    Status Get(const ReadOptions& _read_options, const Slice& key,
      std::string* value) {
      return Get(_read_options, db_->DefaultColumnFamily(), key, value);
    }

    Status Scan(const ReadOptions& _read_options,
      ColumnFamilyHandle* column_family, const Slice &key, int record_count,
      const std::function<void(PinnableSlice*)>& func);
    Status Scan(const ReadOptions& _read_options, const Slice &key, int record_count,
      const std::function<void(PinnableSlice*)>& func) {
      return Scan(_read_options, db_->DefaultColumnFamily(), key, record_count, func);
    }

  private:
    ElasticLSMOptions options_;
    DBElastic* db_;
    ThreadPoolImpl* tp_thread_pool_;
    ThreadPoolImpl* ap_thread_pool_;
    ThreadPoolImpl* background_thread_pool_;
    std::atomic<tp_task*> tp_task_head_, tp_task_tail_;
    std::atomic<ap_task*> ap_task_head_, ap_task_tail_;
    std::atomic<int> tp_task_count_, ap_task_count_;

    int GetBackgroundTaskCount() const {
      return db_->GetBackgroundTaskCount();
    }
    int GetTPThreadsNum() const {
      return tp_thread_pool_->GetBackgroundThreads();
    }
    int GetAPThreadsNum() const {
      return ap_thread_pool_->GetBackgroundThreads();
    }
    int GetBackgroundThreadsNum() const {
      return background_thread_pool_->GetBackgroundThreads();
    }
    void SetTPThreadsNum(int num) {
      tp_thread_pool_->SetBackgroundThreads(num);
    }
    void SetAPThreadsNum(int num) {
      ap_thread_pool_->SetBackgroundThreads(num);
    }
    void SetBackgroundThreadsNum(int num) {
      background_thread_pool_->SetBackgroundThreads(num);
    }
    void AdjustThreadPoolSize();
    void BGTPWork();
    void BGAPWork();
    
    friend class DBElastic;
  };
}