#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>
  // std::cout<<"hhhhhhhhhhhhhhh"<<std::endl;

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    BUSTUB_ENSURE(optimized_plan->GetChildren().size() == 2, "NLJ should have exactly 2 children.");
    if (const auto *expr = dynamic_cast<const ComparisonExpression *>(nlj_plan.Predicate().get()); expr != nullptr) {
      if (expr->comp_type_ == ComparisonType::Equal) {
        BUSTUB_ENSURE(expr->GetChildren().size() == 2, "ComparisonExpression should have exactly 2 children.");
        if (const auto *left_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
            left_expr != nullptr) {
          if (const auto *right_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
              right_expr != nullptr) {
            std::vector<AbstractExpressionRef> left_expr_v;
            std::vector<AbstractExpressionRef> right_expr_v;
            if (left_expr->GetTupleIdx() == 0 && right_expr->GetTupleIdx() == 1) {
              left_expr_v.emplace_back(expr->children_[0]);
              right_expr_v.emplace_back(expr->children_[1]);
            }
            if (left_expr->GetTupleIdx() == 1 && right_expr->GetTupleIdx() == 0) {
              left_expr_v.emplace_back(expr->children_[1]);
              right_expr_v.emplace_back(expr->children_[0]);
            }
            return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                      nlj_plan.GetRightPlan(), left_expr_v, right_expr_v,
                                                      nlj_plan.GetJoinType());
          }
        }
      }
    }
    if (const auto *expr = dynamic_cast<const LogicExpression *>(nlj_plan.Predicate().get()); expr != nullptr) {
      if (expr->logic_type_ == LogicType::And) {
        BUSTUB_ENSURE(expr->GetChildren().size() == 2, "LogicExpression should have exactly 2 children.");
        if (const auto *left_expr = dynamic_cast<const ComparisonExpression *>(expr->children_[0].get());
            left_expr != nullptr) {
          if (const auto *right_expr = dynamic_cast<const ComparisonExpression *>(expr->children_[1].get());
              right_expr != nullptr) {
            BUSTUB_ENSURE(left_expr->GetChildren().size() == 2, "ComparisonExpression should have exactly 2 children.");
            BUSTUB_ENSURE(right_expr->GetChildren().size() == 2,
                          "ComparisonExpression should have exactly 2 children.");
            std::vector<AbstractExpressionRef> left_expr_v;
            std::vector<AbstractExpressionRef> right_expr_v;
            if (const auto *left_expr_l = dynamic_cast<const ColumnValueExpression *>(left_expr->children_[0].get());
                left_expr_l != nullptr) {
              if (const auto *right_expr_l = dynamic_cast<const ColumnValueExpression *>(left_expr->children_[1].get());
                  right_expr_l != nullptr) {
                if (left_expr_l->GetTupleIdx() == 0 && right_expr_l->GetTupleIdx() == 1) {
                  left_expr_v.emplace_back(left_expr->children_[0]);
                  right_expr_v.emplace_back(left_expr->children_[1]);
                }
                if (left_expr_l->GetTupleIdx() == 1 && right_expr_l->GetTupleIdx() == 0) {
                  left_expr_v.emplace_back(left_expr->children_[1]);
                  right_expr_v.emplace_back(left_expr->children_[0]);
                }
                if (const auto *left_expr_r =
                        dynamic_cast<const ColumnValueExpression *>(right_expr->children_[0].get());
                    left_expr_r != nullptr) {
                  if (const auto *right_expr_r =
                          dynamic_cast<const ColumnValueExpression *>(right_expr->children_[1].get());
                      right_expr_r != nullptr) {
                    if (left_expr_r->GetTupleIdx() == 0 && right_expr_r->GetTupleIdx() == 1) {
                      left_expr_v.emplace_back(right_expr->children_[0]);
                      right_expr_v.emplace_back(right_expr->children_[1]);
                    }
                    if (left_expr_r->GetTupleIdx() == 1 && right_expr_r->GetTupleIdx() == 0) {
                      left_expr_v.emplace_back(right_expr->children_[1]);
                      right_expr_v.emplace_back(right_expr->children_[0]);
                    }
                    return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                              nlj_plan.GetRightPlan(), left_expr_v, right_expr_v,
                                                              nlj_plan.GetJoinType());
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub
