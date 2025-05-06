#pragma once

#include "rocksdb/elastic_lsm.h"
#include "db/elastic/db_elastic.h"
#include "db/elastic/task.h"
#include "util/threadpool_imp.h"

namespace ROCKSDB_NAMESPACE 
{
  class ElasticLSMImpl : public ElasticLSM
  {
  public:
    ElasticLSMImpl(const ElasticLSMOptions& elastic_options);
    ~ElasticLSMImpl();
    static Status Open(const DBOptions& db_options, const ElasticLSMOptions& elastic_options,
      const std::string& dbname, const std::vector<ColumnFamilyDescriptor>& column_families,
      std::vector<ColumnFamilyHandle*>* handles,
      std::unique_ptr<ElasticLSM>* dbptr, const bool seq_per_batch,
      const bool batch_per_txn, const bool is_retry,
      bool* can_retry);

    Status Put(const WriteOptions& options, ColumnFamilyHandle* column_family,
      const Slice& key, const Slice& value);

    Status Delete(const WriteOptions& options,
      ColumnFamilyHandle* column_family,
      const Slice& key);

    Status Update(const WriteOptions& options, ColumnFamilyHandle* column_family,
      const Slice& key, const Slice& value);

    Status Get(const ReadOptions& _read_options,
      ColumnFamilyHandle* column_family, const Slice& key,
      std::string* value);

    Status Scan(const ReadOptions& _read_options,
      ColumnFamilyHandle* column_family, const Slice &key, int record_count,
      const std::function<void(PinnableSlice*)>& func);
    
    ColumnFamilyHandle* DefaultColumnFamily() const override{
      return db_->DefaultColumnFamily();
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