#include "db/elastic/elastic_lsm.h"

namespace ROCKSDB_NAMESPACE { 
  ElasticLSM::ElasticLSM() {

  }

  Status ElasticLSM::Open(const DBOptions& db_options, const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles,
    std::unique_ptr<DB>* dbptr, const bool seq_per_batch,
    const bool batch_per_txn, const bool is_retry,
    bool* can_retry) {
      std::unique_ptr<DB> db;
      DBElastic* dbelastic;

      auto s = DBImpl::Open(db_options, dbname, column_families, handles,
                            &db, seq_per_batch, batch_per_txn,
                            is_retry, can_retry);
      if (!s.ok()) { 
        return s; 
      }

      DBElastic* dbelastic = dynamic_cast<DBElastic*>(db.get());
      if (!dbelastic) { 
        return Status::InvalidArgument("Underlying DB is not DBElastic"); 
      }

      // Wrap in ElasticLSM
      ElasticLSM* elastic = new ElasticLSM();
      elastic->db_ = dbelastic;
      dbelastic->elastic_lsm_ = elastic;
      db.release();

      // Initialize thread pool
      elastic->tp_thread_pool_ = new ThreadPoolImpl();
      elastic->tp_thread_pool_->SetBackgroundThreads(options->min_tp_threads);

      elastic->ap_thread_pool_ = new ThreadPoolImpl();
      elastic->ap_thread_pool_->SetBackgroundThreads(options->min_ap_threads);

      elastic->background_thread_pool_ = new ThreadPoolImpl();
      elastic->background_thread_pool_->SetBackgroundThreads(options->min_compaction_threads);

      // Return as DB interface
      dbptr->reset(elastic);
      
      return Status::OK();
  }

  Status ElasticLSM::Put(const WriteOptions& options, ColumnFamilyHandle* column_family,
    const Slice& key, const Slice& value) {
      // Create task
      auto* t = new put_task(options, column_family, key, value);

      // Insert into the end of the tp task linked list
      tp_task* prev_tail = tp_task_tail_.exchange(t, std::memory_order_acq_rel);
      if (prev_tail) {
        prev_tail->next_task = t;
        t->prev_task = prev_tail;
      } else {
        tp_task_head_.store(t, std::memory_order_release);
      }
      tp_task_count_.fetch_add(1, std::memory_order_relaxed);

      return Status::OK();
  }

  Status ElasticLSM::Delete(const WriteOptions& options,
    ColumnFamilyHandle* column_family,
    const Slice& key) {
      auto* t = new delete_task(options, column_family, key);

      tp_task* prev_tail = tp_task_tail_.exchange(t, std::memory_order_acq_rel);
      if (prev_tail) {
        prev_tail->next_task = t;
        t->prev_task = prev_tail;
      } else {
        tp_task_head_.store(t, std::memory_order_release);
      }
      tp_task_count_.fetch_add(1, std::memory_order_relaxed);

      return Status::OK();
  }

  Status ElasticLSM::Update(const WriteOptions& options, ColumnFamilyHandle* column_family,
    const Slice& key, const Slice& value) {
      auto* t = new update_task(options, column_family, key, value);

      tp_task* prev_tail = tp_task_tail_.exchange(t, std::memory_order_acq_rel);
      if (prev_tail) {
        prev_tail->next_task = t;
        t->prev_task = prev_tail;
      } else {
        tp_task_head_.store(t, std::memory_order_release);
      }
      tp_task_count_.fetch_add(1, std::memory_order_relaxed);

      return Status::OK();
  }

  Status ElasticLSM::Get(const ReadOptions& _read_options,
    ColumnFamilyHandle* column_family, const Slice& key,
    std::string* value) {
      auto* t = new get_task(_read_options, column_family, key, value);

      tp_task* prev_tail = tp_task_tail_.exchange(t, std::memory_order_acq_rel);
      if (prev_tail) {
        prev_tail->next_task = t;
        t->prev_task = prev_tail;
      } else {
        tp_task_head_.store(t, std::memory_order_release);
      }
      tp_task_count_.fetch_add(1, std::memory_order_relaxed);

      return Status::OK();
  }

  Status ElasticLSM::Scan(const ReadOptions& _read_options,
    ColumnFamilyHandle* column_family, const Slice &key, int record_count,
    const std::function<void(PinnableSlice*)>& func) {
      auto* t = new ap_task(_read_options, column_family, key, record_count, func);

      ap_task* prev_tail = ap_task_tail_.exchange(t, std::memory_order_acq_rel);
      if (prev_tail) {
        prev_tail->next_task = t;
        t->prev_task = prev_tail;
      } else {
        ap_task_head_.store(t, std::memory_order_release);
      }
      ap_task_count_.fetch_add(1, std::memory_order_relaxed);

      return Status::OK();
  }
  void ElasticLSM::AdjustThreadPoolSize() {

  }

  void ElasticLSM::BGTPWork() {

  }

  void ElasticLSM::BGAPWork() {

  }

}