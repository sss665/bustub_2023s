//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  status1_ = left_executor_->Next(&left_tuple_, &left_rid_);
  done_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto filter_expr = plan_->Predicate();
  Tuple right_tuple;
  RID right_rid;
  for (; status1_; status1_ = left_executor_->Next(&left_tuple_, &left_rid_)) {
    for (bool status2 = right_executor_->Next(&right_tuple, &right_rid); status2;
         status2 = right_executor_->Next(&right_tuple, &right_rid)) {
      auto value = filter_expr->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple,
                                             right_executor_->GetOutputSchema());
      if (!value.IsNull() && value.GetAs<bool>()) {
        std::vector<Value> values;
        for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        // 连接操作右边元组的值均不为null的情况下
        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
          values.emplace_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
        }
        *tuple = {values, &GetOutputSchema()};
        if (plan_->GetJoinType() == JoinType::LEFT) {
          done_ = true;
        }
        return true;
      }
    }
    right_executor_->Init();
    if (plan_->GetJoinType() == JoinType::LEFT && !done_) {
      std::vector<Value> values;
      for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.emplace_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
      }
      for (const Column &col : right_executor_->GetOutputSchema().GetColumns()) {
        values.emplace_back(ValueFactory::GetNullValueByType(col.GetType()));
      }
      *tuple = {values, &GetOutputSchema()};
      status1_ = left_executor_->Next(&left_tuple_, &left_rid_);
      return true;
    }
    done_ = false;
    // Get the next tuple
  }
  return false;
}

}  // namespace bustub
