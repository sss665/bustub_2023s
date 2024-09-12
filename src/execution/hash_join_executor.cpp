//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  Tuple tuple;
  RID rid;
  while (true) {
    // Get the next tuple
    const auto status = right_child_->Next(&tuple, &rid);
    if (!status) {
      break;
    }
    HashJoinKey hash_join_key = MakeRightHashJoinKey(&tuple);
    hjt_.InsertCombine(hash_join_key, tuple);
  }
  have_found_ = false;
  index_ = 0;
  hash_join_key_ = HashJoinKey();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (have_found_) {
    if (!hjt_.IfEnd(hash_join_key_, index_)) {
      index_++;
      auto right_tuple = hjt_.FindValue(hash_join_key_, index_);
      std::vector<Value> values;
      for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(left_tuple_.GetValue(&left_child_->GetOutputSchema(), i));
      }
      // 连接操作右边元组的值均不为null的情况下
      for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(right_tuple.GetValue(&right_child_->GetOutputSchema(), i));
      }
      *tuple = {values, &GetOutputSchema()};
      return true;
    }
  }
  Tuple left_tuple;
  RID left_rid;
  have_found_ = false;
  index_ = 0;
  hash_join_key_ = HashJoinKey();
  while (true) {
    const auto status = left_child_->Next(&left_tuple, &left_rid);
    if (!status) {
      return false;
    }
    HashJoinKey hash_join_key = MakeLeftHashJoinKey(&left_tuple);
    if (hjt_.Find(hash_join_key) != hjt_.End()) {
      auto right_tuple = hjt_.FindValue(hash_join_key, 0);
      std::vector<Value> values;
      for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(left_tuple.GetValue(&left_child_->GetOutputSchema(), i));
      }
      // 连接操作右边元组的值均不为null的情况下
      for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(right_tuple.GetValue(&right_child_->GetOutputSchema(), i));
      }
      *tuple = {values, &GetOutputSchema()};
      have_found_ = true;
      index_ = 0;
      hash_join_key_ = hash_join_key;
      left_tuple_ = left_tuple;
      return true;
    }
    if (plan_->GetJoinType() == JoinType::LEFT) {
      break;
    }
  }
  if (plan_->GetJoinType() == JoinType::LEFT) {
    std::vector<Value> values;
    for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumnCount(); i++) {
      values.emplace_back(left_tuple.GetValue(&left_child_->GetOutputSchema(), i));
    }
    for (const Column &col : right_child_->GetOutputSchema().GetColumns()) {
      values.emplace_back(ValueFactory::GetNullValueByType(col.GetType()));
    }
    *tuple = {values, &GetOutputSchema()};
    return true;
  }
  return false;
}

}  // namespace bustub
