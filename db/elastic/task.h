#pragma once

#include "db/elastic/db_elastic.h"

namespace ROCKSDB_NAMESPACE {
  struct task
  {
    enum task_type
    {
      TASK_TYPE_NONE,
      TASK_TYPE_TP,
      TASK_TYPE_AP,
    } type;
    task* prev_task;
    task* next_task;
    
   public:
    task(task_type _type = TASK_TYPE_NONE, task* _prev_task = nullptr, task* _next_task = nullptr)
        : type(_type), prev_task(_prev_task), next_task(_next_task) {}
  };
  
  struct tp_task : public task
  {
    enum tp_task_type
    {
      TP_TASK_TYPE_NONE,
      TP_TASK_TYPE_PUT,
      TP_TASK_TYPE_DELETE,
      TP_TASK_TYPE_UPDATE,
      TP_TASK_TYPE_GET
    } tp_type;
    
   public:
    tp_task(tp_task_type _tp_type = TP_TASK_TYPE_NONE, 
            task_type _type = TASK_TYPE_TP, 
            task* _prev_task = nullptr, 
            task* _next_task = nullptr)
        : task(_type, _prev_task, _next_task), tp_type(_tp_type) {}
  };
  
  struct put_task : public tp_task
  {
    const WriteOptions& write_options;
    ColumnFamilyHandle* column_family;
    const Slice& key;
    const Slice& value;
    
   public:
    put_task(const WriteOptions& _write_options, 
             ColumnFamilyHandle* _column_family,
             const Slice& _key, const Slice& _value,
             tp_task_type _tp_type = TP_TASK_TYPE_PUT,
             task* _prev_task = nullptr,
             task* _next_task = nullptr)
        : tp_task(_tp_type, TASK_TYPE_TP, _prev_task, _next_task), 
          write_options(_write_options),
          column_family(_column_family),
          key(_key), value(_value) {}
  };
  
  struct delete_task : public tp_task
  {
    const WriteOptions& write_options;
    ColumnFamilyHandle* column_family;
    const Slice& key;
    
   public:
    delete_task(const WriteOptions& _write_options,
                ColumnFamilyHandle* _column_family,
                const Slice& _key,
                tp_task_type _tp_type = TP_TASK_TYPE_DELETE,
                task* _prev_task = nullptr,
                task* _next_task = nullptr)
        : tp_task(_tp_type, TASK_TYPE_TP, _prev_task, _next_task),
          write_options(_write_options),
          column_family(_column_family),
          key(_key) {}
  };
  
  struct update_task : public tp_task
  {
    const WriteOptions& write_options;
    ColumnFamilyHandle* column_family;
    const Slice& key;
    const Slice& value;
    
   public:
    update_task(const WriteOptions& _write_options,
                ColumnFamilyHandle* _column_family,
                const Slice& _key, const Slice& _value,
                tp_task_type _tp_type = TP_TASK_TYPE_UPDATE,
                task* _prev_task = nullptr,
                task* _next_task = nullptr)
        : tp_task(_tp_type, TASK_TYPE_TP, _prev_task, _next_task),
          write_options(_write_options),
          column_family(_column_family),
          key(_key), value(_value) {}
  };
  
  struct get_task : public tp_task
  {
    const ReadOptions& read_options;
    ColumnFamilyHandle* column_family;
    const Slice& key;
    std::string* value;
    
   public:
    get_task(const ReadOptions& _read_options,
             ColumnFamilyHandle* _column_family,
             const Slice& _key, std::string* _value,
             tp_task_type _tp_type = TP_TASK_TYPE_GET,
             task* _prev_task = nullptr,
             task* _next_task = nullptr)
        : tp_task(_tp_type, TASK_TYPE_TP, _prev_task, _next_task),
          read_options(_read_options),
          column_family(_column_family),
          key(_key), value(_value) {}
  };
  
  struct ap_task : public task
  {
    const ReadOptions& read_options;
    ColumnFamilyHandle* column_family;
    const Slice& key;
    int record_count;
    std::function<void(PinnableSlice*)> func;
    
   public:
    ap_task(const ReadOptions& _read_options,
            ColumnFamilyHandle* _column_family,
            const Slice& _key, int _record_count,
            std::function<void(PinnableSlice*)> _func,
            task_type _type = TASK_TYPE_AP,
            task* _prev_task = nullptr,
            task* _next_task = nullptr)
        : task(_type, _prev_task, _next_task),
          read_options(_read_options),
          column_family(_column_family),
          key(_key), record_count(_record_count),
          func(_func) {}
  };
}