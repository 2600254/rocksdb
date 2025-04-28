#pragma once

#include "db/db_impl/db_impl.h"

namespace ROCKSDB_NAMESPACE {
  class ElasticLSM;
  class DBElastic : public DBImpl {
  public:
    int GetBackgroundTaskCount() const {
      return unscheduled_flushes_ + unscheduled_compactions_;
    }
  protected:
    void MaybeScheduleFlushOrCompaction();
  private:
    ElasticLSM* elastic_lsm_;
  };
}