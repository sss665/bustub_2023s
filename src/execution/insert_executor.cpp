//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())),
      table_index_(exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)),
      child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  done_ = false;
  LockManager *lock_manager = exec_ctx_->GetLockManager();
  lock_manager->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE, table_info_->oid_);
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  // auto filter_expr = plan_->GetPredicate();
  // Get the next tuple
  if (done_) {
    return false;
  }
  int32_t col = 0;
  TupleMeta insert_tuple_meta{INVALID_TXN_ID, INVALID_TXN_ID, false};
  Tuple insert_tuple;
  RID common_rid;
  while (true) {
    // Get the next tuple
    const auto status = child_executor_->Next(&insert_tuple, &common_rid);
    if (!status) {
      break;
    }
    auto insert_rid = table_info_->table_->InsertTuple(insert_tuple_meta, insert_tuple, exec_ctx_->GetLockManager(),
                                                       exec_ctx_->GetTransaction(), table_info_->oid_);
    TableWriteRecord table_write_record = TableWriteRecord(table_info_->oid_, *insert_rid, table_info_->table_.get());
    table_write_record.wtype_ = WType::INSERT;
    exec_ctx_->GetTransaction()->AppendTableWriteRecord(table_write_record);
    for (const auto &indexs : table_index_) {
      indexs->index_->InsertEntry(
          insert_tuple.KeyFromTuple(table_info_->schema_, indexs->key_schema_, indexs->index_->GetKeyAttrs()),
          *insert_rid, exec_ctx_->GetTransaction());
    }
    col++;
  }
  done_ = true;
  *tuple = Tuple({Value(INTEGER, col)}, &GetOutputSchema());
  return true;
}

}  // namespace bustub
