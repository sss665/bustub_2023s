//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
class SimpleHashJoinTable {
 public:
	/**
	 * Construct a new SimpleAggregationHashTable instance.
	 * @param agg_exprs the aggregation expressions
	 * @param agg_types the types of aggregations
	 */
	SimpleHashJoinTable() = default;
	/**
	 * TODO(Student)
	 *
	 * Combines the input into the aggregation result.
	 * @param[out] result The output aggregate value
	 * @param input The input value
	 */
	/*void CombineAggregateValues(HashJoinValue *result, const Tuple &input) {
		result->tuples.emplace_back(input);
	}*/

	void InsertCombine(const HashJoinKey &hash_key, const Tuple &val) {
		if (ht_.count(hash_key) == 0) {
			std::vector<Tuple> tuple{val};
			ht_.insert({hash_key, {tuple}});
		}else{
			ht_[hash_key].tuples.emplace_back(val);
		}
	}

	auto FindValue(const HashJoinKey &hash_key,int index) -> Tuple{
		return ht_[hash_key].tuples[index];
	}

	auto IfEnd(const HashJoinKey &hash_key,uint32_t index) -> bool{
		if(index == ht_[hash_key].tuples.size()-1){
			return true;
		}
		return false;
	}

	/**
	 * Clear the hash table
	 */
	void Clear() { ht_.clear(); }
	auto End() -> std::unordered_map<HashJoinKey, HashJoinValue>::const_iterator { return ht_.cend(); }
	auto Find(HashJoinKey hash_join_key) -> std::unordered_map<HashJoinKey, HashJoinValue>::const_iterator { return ht_.find(hash_join_key); }
 private:
	/** The hash table is just a map from aggregate keys to aggregate values */
	std::unordered_map<HashJoinKey, HashJoinValue> ht_{};
	/** The types of aggregations that we have */
};
/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
	/** @return The tuple as an AggregateKey */
	auto MakeLeftHashJoinKey(const Tuple *tuple) -> HashJoinKey {
		std::vector<Value> keys;
		for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
			keys.emplace_back(expr->Evaluate(tuple, left_child_->GetOutputSchema()));
		}
		return {keys};
	}

	/** @return The tuple as an AggregateValue */
	auto MakeRightHashJoinKey(const Tuple *tuple) -> HashJoinKey {
		std::vector<Value> keys;
		for (const auto &expr : plan_->RightJoinKeyExpressions()) {
			keys.emplace_back(expr->Evaluate(tuple, right_child_->GetOutputSchema()));
		}
		return {keys};
	}
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
	std::unique_ptr<AbstractExecutor> left_child_;
	std::unique_ptr<AbstractExecutor> right_child_;
	SimpleHashJoinTable hjt_;
	HashJoinKey hash_join_key_;
	uint32_t index_;
	bool have_fonud_;
	Tuple left_tuple_;
};

}  // namespace bustub
