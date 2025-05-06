#include "db/elastic/elastic_lsm.h"

namespace ROCKSDB_NAMESPACE { 
  ElasticLSM::~ElasticLSM() = default;
  Status ElasticLSM::Put(const WriteOptions& options, ColumnFamilyHandle* column_family,
    const Slice& key, const Slice& value) {}
    
  Status ElasticLSM::Delete(const WriteOptions& options,
      ColumnFamilyHandle* column_family,
      const Slice& key) {}
      
  Status ElasticLSM::Update(const WriteOptions& options, ColumnFamilyHandle* column_family,
      const Slice& key, const Slice& value) {}

  Status ElasticLSM::Get(const ReadOptions& _read_options,
      ColumnFamilyHandle* column_family, const Slice& key,
      std::string* value) {}

  Status ElasticLSM::Scan(const ReadOptions& _read_options,
      ColumnFamilyHandle* column_family, const Slice &key, int record_count,
      const std::function<void(PinnableSlice*)>& func) {}

  ElasticLSMImpl::ElasticLSMImpl(const ElasticLSMOptions& elastic_options) :
    options_(elastic_options) {

  }
  
  ElasticLSMImpl::~ElasticLSMImpl() {

  }

  Status ElasticLSMImpl::Open(const DBOptions& db_options, const ElasticLSMOptions& elastic_options,
    const std::string& dbname, const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles,
    std::unique_ptr<ElasticLSM>* dbptr, const bool seq_per_batch,
    const bool batch_per_txn, const bool is_retry,
    bool* can_retry) {
      std::unique_ptr<DB> db;
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
      ElasticLSMImpl* elastic = new ElasticLSMImpl(elastic_options);
      elastic->db_ = dbelastic;
      dbelastic->elastic_lsm_ = elastic;
      db.release();

      // Initialize thread pool
      elastic->tp_thread_pool_ = new ThreadPoolImpl();
      elastic->tp_thread_pool_->SetBackgroundThreads(elastic->options_.min_tp_threads);

      elastic->ap_thread_pool_ = new ThreadPoolImpl();
      elastic->ap_thread_pool_->SetBackgroundThreads(elastic->options_.min_ap_threads);

      elastic->background_thread_pool_ = new ThreadPoolImpl();
      elastic->background_thread_pool_->SetBackgroundThreads(elastic->options_.min_compaction_threads);

      // Return as DB interface
      dbptr->reset(elastic);
      
      return Status::OK();
  }

  Status ElasticLSMImpl::Put(const WriteOptions& options, ColumnFamilyHandle* column_family,
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

  Status ElasticLSMImpl::Delete(const WriteOptions& options,
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

  Status ElasticLSMImpl::Update(const WriteOptions& options, ColumnFamilyHandle* column_family,
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

  Status ElasticLSMImpl::Get(const ReadOptions& _read_options,
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

  Status ElasticLSMImpl::Scan(const ReadOptions& _read_options,
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
  void ElasticLSMImpl::AdjustThreadPoolSize() {
    // Current number of threads
    int cur_tp = GetTPThreadsNum();
    int cur_ap = GetAPThreadsNum();
    int cur_bg = GetBackgroundThreadsNum();

    // Limiting tool: Take values within the range of [low, high]
    auto clamp = [&](int n, int low, int high) {
      return std::max(low, std::min(high, n));
    };

    // When the number of pending tasks exceeds the current number of threads, it expands;
    // When drops sharply, it shrinks
    // TP thread pool
    if (pending_tp > cur_tp && cur_tp < options_.max_background_threads) {
      int nt = clamp(cur_tp * 2,
                     options_.min_tp_threads,
                     options_.max_background_threads);
      SetTPThreadsNum(nt);
    } else if (pending_tp < cur_tp / 2 && cur_tp > options_.min_tp_threads) {
      int nt = clamp(cur_tp / 2,
                     options_.min_tp_threads,
                     options_.max_background_threads);
      SetTPThreadsNum(nt);
    }

    // AP thread pool
    if (pending_ap > cur_ap && cur_ap < options_.max_background_threads) {
      int na = clamp(cur_ap * 2,
                     options_.min_ap_threads,
                     options_.max_background_threads);
      SetAPThreadsNum(na);
    } else if (pending_ap < cur_ap / 2 && cur_ap > options_.min_ap_threads) {
      int na = clamp(cur_ap / 2,
                     options_.min_ap_threads,
                     options_.max_background_threads);
      SetAPThreadsNum(na);
    }

    // Compaction thread pool
    if (pending_bg > cur_bg && cur_bg < options_.max_background_threads) {
      int nb = clamp(cur_bg * 2,
                     options_.min_compaction_threads,
                     options_.max_background_threads);
      SetBackgroundThreadsNum(nb);
    } else if (pending_bg < cur_bg / 2 &&
               cur_bg > options_.min_compaction_threads) {
      int nb = clamp(cur_bg / 2,
                     options_.min_compaction_threads,
                     options_.max_background_threads);
      SetBackgroundThreadsNum(nb);
    }
  }

  void ElasticLSMImpl::BGTPWork() {
    AdjustThreadPoolSize();

    // Pop up a task without locking
    tp_task* task = nullptr;
    while (true) {
      tp_task* head = tp_task_head_.load(std::memory_order_acquire);
      if (head == nullptr) {
        break;
      }
      tp_task* next = head->next_task;
      if (tp_task_head_.compare_exchange_strong(
              head, next,
              std::memory_order_acq_rel,
              std::memory_order_acquire)) {
        if (next == nullptr) {
          tp_task_tail_.store(nullptr, std::memory_order_release);
        } else {
          next->prev_task = nullptr;
        }
        tp_task_count_.fetch_sub(1, std::memory_order_relaxed);
        task = head;
        break;
      }

      if (task) {
        // Call the corresponding DBImpl interface according to the Task type
        switch (task->tp_type) {
          case tp_task::TP_TASK_TYPE_PUT: {
            auto* t = static_cast<put_task*>(task);
            db_->DBImpl::Put(t->write_options,
                             t->column_family,
                             t->key,
                             t->value);
            break;
          }
          case tp_task::TP_TASK_TYPE_DELETE: {
            auto* t = static_cast<delete_task*>(task);
            db_->DBImpl::Delete(t->write_options,
                                t->column_family,
                                t->key);
            break;
          }
          case tp_task::TP_TASK_TYPE_UPDATE: {
            auto* t = static_cast<update_task*>(task);
            db_->DBImpl::Put(t->write_options,
                             t->column_family,
                             t->key,
                             t->value);
            break;
          }
          case tp_task::TP_TASK_TYPE_GET: {
            auto* t = static_cast<get_task*>(task);
            db_->DBImpl::Get(t->read_options,
                             t->column_family,
                             t->key,
                             t->value);
            break;
          }
          default:
            break;
        }
        delete task;
    
        // If there are still residual tasks in the queue, self-drive to schedule again
        if (tp_task_count_.load(std::memory_order_relaxed) > 0) {
          tp_thread_pool_->SubmitJob(
              std::bind(&ElasticLSM::BGTPWork, this));
        }
      }
    }
  }

  void ElasticLSMImpl::BGAPWork() {
    AdjustThreadPoolSize();

    ap_task* task = nullptr;
    while (true) {
      ap_task* head = ap_task_head_.load(std::memory_order_acquire);
      if (head == nullptr) {
        break;
      }
      ap_task* next = head->next_task;
      if (ap_task_head_.compare_exchange_strong(
              head, next,
              std::memory_order_acq_rel,
              std::memory_order_acquire)) {
        if (next == nullptr) {
          ap_task_tail_.store(nullptr, std::memory_order_release);
        } else {
          next->prev_task = nullptr;
        }
        ap_task_count_.fetch_sub(1, std::memory_order_relaxed);
        task = head;
        break;
      }
    }

    if (task) {
      // Perform range scanning using iterators and process each result through callbacks
      auto* it = db_->NewIterator(task->read_options,
                                  task->column_family);
      it->Seek(task->key);
      int cnt = 0;
      while (it->Valid() && cnt < task->record_count) {
        auto* ps = new PinnableSlice();
        ps->PinSlice(it->value());
        task->func(ps);
        delete ps;
        it->Next();
        ++cnt;
      }
      delete it;
      delete task;

      if (ap_task_count_.load(std::memory_order_relaxed) > 0) {
        ap_thread_pool_->SubmitJob(
            std::bind(&ElasticLSM::BGAPWork, this));
      }
    }
  }

}