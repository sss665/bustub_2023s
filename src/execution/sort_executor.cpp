#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_->Init();
  tuples_.clear();
  Tuple tuple;
  RID rid;
  while (true) {
    const auto status = child_->Next(&tuple, &rid);
    if (!status) {
      break;
    }
    tuples_.emplace_back(tuple);
  }
  std::sort(tuples_.begin(), tuples_.end(), [this](const Tuple &tuple_1, const Tuple &tuple_2) -> bool {
    for (const auto &pairs : plan_->GetOrderBy()) {
      auto value1 = pairs.second->Evaluate(&tuple_1, child_->GetOutputSchema());
      auto value2 = pairs.second->Evaluate(&tuple_2, child_->GetOutputSchema());
      switch (pairs.first) {
        case OrderByType::INVALID:
          std::cerr << "invalid OrderByType" << std::endl;
          break;
        case OrderByType::DEFAULT:
        case OrderByType::ASC:
          if (value1.CompareLessThan(value2) == CmpBool::CmpTrue) {
            return true;
          }
          if (value1.CompareGreaterThan(value2) == CmpBool::CmpTrue) {
            return false;
          }
          break;
        case OrderByType::DESC:
          if (value1.CompareLessThan(value2) == CmpBool::CmpTrue) {
            return false;
          }
          if (value1.CompareGreaterThan(value2) == CmpBool::CmpTrue) {
            return true;
          }
          break;
      }
    }
    return true;
  });
  it_ = tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (it_ == tuples_.end()) {
    return false;
  }
  *tuple = *it_;
  *rid = RID(INVALID_PAGE_ID, 0);
  ++it_;
  return true;
}

}  // namespace bustub
