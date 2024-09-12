//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())),
      table_index_(exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  done_ = false;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (done_) {
    return false;
  }
  int32_t col = 0;
  Tuple delete_tuple;
  RID delete_rid;
  while (true) {
    // Get the next tuple
    const auto status = child_executor_->Next(&delete_tuple, &delete_rid);
    if (!status) {
      break;
    }
    auto tuple_meta = table_info_->table_->GetTupleMeta(delete_rid);
    tuple_meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(tuple_meta, delete_rid);
    TableWriteRecord table_write_record = TableWriteRecord(table_info_->oid_, delete_rid, table_info_->table_.get());
    table_write_record.wtype_ = WType::DELETE;
    exec_ctx_->GetTransaction()->AppendTableWriteRecord(table_write_record);
    for (const auto &indexs : table_index_) {
      indexs->index_->DeleteEntry(
          delete_tuple.KeyFromTuple(table_info_->schema_, indexs->key_schema_, indexs->index_->GetKeyAttrs()),
          delete_rid, exec_ctx_->GetTransaction());
    }
    col++;
  }
  *tuple = Tuple({Value(INTEGER, col)}, &GetOutputSchema());
  done_ = true;
  return true;
}

}  // namespace bustub
