//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())),
      table_index_(exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)),
      child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() { child_executor_->Init(); }

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (done_) {
    return false;
  }
  int32_t num = 0;
  Tuple update_tuple;
  RID update_rid;
  TupleMeta insert_tuple_meta{INVALID_TXN_ID, INVALID_TXN_ID, false};
  while (true) {
    // Get the next tuple
    const auto status = child_executor_->Next(&update_tuple, &update_rid);
    if (!status) {
      break;
    }
    // std::cout<<updateTuple.ToString(&child_executor_->GetOutputSchema())<<std::endl;
    auto tuple_meta = table_info_->table_->GetTupleMeta(update_rid);
    tuple_meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(tuple_meta, update_rid);
    std::vector<Value> values;
    for (auto &expr : plan_->target_expressions_) {
      values.emplace_back(expr->Evaluate(&update_tuple, child_executor_->GetOutputSchema()));
    }
    Tuple insert_tuple(values, &table_info_->schema_);
    auto insert_rid = table_info_->table_->InsertTuple(insert_tuple_meta, insert_tuple);
    for (const auto &indexs : table_index_) {
      indexs->index_->DeleteEntry(
          update_tuple.KeyFromTuple(table_info_->schema_, indexs->key_schema_, indexs->index_->GetKeyAttrs()),
          update_rid, exec_ctx_->GetTransaction());
      indexs->index_->InsertEntry(
          insert_tuple.KeyFromTuple(table_info_->schema_, indexs->key_schema_, indexs->index_->GetKeyAttrs()),
          *insert_rid, exec_ctx_->GetTransaction());
    }
    num++;
  }
  *tuple = Tuple({Value(INTEGER, num)}, &GetOutputSchema());
  done_ = true;
  return true;
}

}  // namespace bustub
