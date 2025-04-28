#include "db/elastic/elastic_lsm.h"

namespace ROCKSDB_NAMESPACE { 
  ElasticLSM::ElasticLSM() {

  }
  Status ElasticLSM::Open(const Options& options, const std::string& name,
    std::unique_ptr<DB>*  dbptr) {
      
  }

  Status ElasticLSM::Put(const WriteOptions& options, ColumnFamilyHandle* column_family,
    const Slice& key, const Slice& value) {

  }

  Status ElasticLSM::Delete(const WriteOptions& options,
    ColumnFamilyHandle* column_family,
    const Slice& key) {

  }

  Status ElasticLSM::Update(const WriteOptions& options, ColumnFamilyHandle* column_family,
    const Slice& key, const Slice& value) {

  }

  Status ElasticLSM::Get(const ReadOptions& _read_options,
    ColumnFamilyHandle* column_family, const Slice& key,
    std::string* value) {

  }

  Status ElasticLSM::Scan(const ReadOptions& _read_options,
    ColumnFamilyHandle* column_family, const Slice &key, int record_count,
    const std::function<void(PinnableSlice*)>& func) {

  }
  void ElasticLSM::AdjustThreadPoolSize() {

  }

  void ElasticLSM::BGTPWork() {

  }

  void ElasticLSM::BGAPWork() {

  }

}