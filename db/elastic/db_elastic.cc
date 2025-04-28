#include "db/elastic/db_elastic.h"

namespace ROCKSDB_NAMESPACE {
    void DBElastic::MaybeScheduleFlushOrCompaction() {
        mutex_.AssertHeld();
        TEST_SYNC_POINT("DBImpl::MaybeScheduleFlushOrCompaction:Start");
        if (!opened_successfully_) {
          // Compaction may introduce data race to DB open
          return;
        }
        if (bg_work_paused_ > 0) {
          // we paused the background work
          return;
        } else if (error_handler_.IsBGWorkStopped() &&
                   !error_handler_.IsRecoveryInProgress()) {
          // There has been a hard error and this call is not part of the recovery
          // sequence. Bail out here so we don't get into an endless loop of
          // scheduling BG work which will again call this function
          //
          // Note that a non-recovery flush can still be scheduled if
          // error_handler_.IsRecoveryInProgress() returns true. We rely on
          // BackgroundCallFlush() to check flush reason and drop non-recovery
          // flushes.
          return;
        } else if (shutting_down_.load(std::memory_order_acquire)) {
          // DB is being deleted; no more background compactions
          return;
        }
        auto bg_job_limits = GetBGJobLimits();
        bool is_flush_pool_empty =
            env_->GetBackgroundThreads(Env::Priority::HIGH) == 0;
        while (!is_flush_pool_empty && unscheduled_flushes_ > 0 &&
               bg_flush_scheduled_ < bg_job_limits.max_flushes) {
          bg_flush_scheduled_++;
          FlushThreadArg* fta = new FlushThreadArg;
          fta->db_ = this;
          fta->thread_pri_ = Env::Priority::HIGH;
          
          env_->Schedule(&DBImpl::BGWorkFlush, fta, Env::Priority::HIGH, this,
                         &DBImpl::UnscheduleFlushCallback);
          --unscheduled_flushes_;
        }
      
        // special case -- if high-pri (flush) thread pool is empty, then schedule
        // flushes in low-pri (compaction) thread pool.
        if (is_flush_pool_empty) {
          while (unscheduled_flushes_ > 0 &&
                 bg_flush_scheduled_ + bg_compaction_scheduled_ <
                     bg_job_limits.max_flushes) {
            bg_flush_scheduled_++;
            FlushThreadArg* fta = new FlushThreadArg;
            fta->db_ = this;
            fta->thread_pri_ = Env::Priority::LOW;
            env_->Schedule(&DBImpl::BGWorkFlush, fta, Env::Priority::LOW, this,
                           &DBImpl::UnscheduleFlushCallback);
            --unscheduled_flushes_;
          }
        }
      
        if (bg_compaction_paused_ > 0) {
          // we paused the background compaction
          return;
        } else if (error_handler_.IsBGWorkStopped()) {
          // Compaction is not part of the recovery sequence from a hard error. We
          // might get here because recovery might do a flush and install a new
          // super version, which will try to schedule pending compactions. Bail
          // out here and let the higher level recovery handle compactions
          return;
        }
      
        if (HasExclusiveManualCompaction()) {
          // only manual compactions are allowed to run. don't schedule automatic
          // compactions
          TEST_SYNC_POINT("DBImpl::MaybeScheduleFlushOrCompaction:Conflict");
          return;
        }
      
        while (bg_compaction_scheduled_ + bg_bottom_compaction_scheduled_ <
                   bg_job_limits.max_compactions &&
               unscheduled_compactions_ > 0) {
          CompactionArg* ca = new CompactionArg;
          ca->db = this;
          ca->compaction_pri_ = Env::Priority::LOW;
          ca->prepicked_compaction = nullptr;
          bg_compaction_scheduled_++;
          unscheduled_compactions_--;
          env_->Schedule(&DBImpl::BGWorkCompaction, ca, Env::Priority::LOW, this,
                         &DBImpl::UnscheduleCompactionCallback);
        }
    }
}