//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
		plan_(plan),
		child_(std::move(child)),
		aht_(plan_->GetAggregates(),plan_->GetAggregateTypes()),
		aht_iterator_(aht_.Begin()),
		done_(false){}

void AggregationExecutor::Init() {
	child_->Init();
	aht_.Clear();
	Tuple aggTuple;
	RID aggRid;
	// std::cout<<plan_->GetAggregates().size()<<" "<<plan_->GetAggregateTypes().size()<<std::endl;
	while (true) {
		// Get the next tuple
		const auto status = child_->Next(&aggTuple, &aggRid);
		if (!status) {
			break;
		}
		AggregateKey aggregate_key = MakeAggregateKey(&aggTuple);
		AggregateValue aggregate_value = MakeAggregateValue(&aggTuple);
		aht_.InsertCombine(aggregate_key,aggregate_value);
	}
	aht_iterator_ = aht_.Begin();
	done_ = false;
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
	if(aht_.Begin()!=aht_.End()){
		if(aht_iterator_==aht_.End()){
			return false;
		}
		std::vector<Value> outPutValue(aht_iterator_.Key().group_bys_.begin(), aht_iterator_.Key().group_bys_.end());
		outPutValue.insert(outPutValue.end(),aht_iterator_.Val().aggregates_.begin(),aht_iterator_.Val().aggregates_.end());
		*tuple = {outPutValue, &GetOutputSchema()};
		++aht_iterator_;
		return true;
	}
	// std::cout<<plan_->GetGroupBys().size()<<std::endl;
	if(done_){
		return false;
	}
	if(GetOutputSchema().GetColumnCount()!=plan_->GetAggregates().size()){
		return false;
	}
	*tuple = {aht_.GenerateInitialAggregateValue().aggregates_,&GetOutputSchema()};
	done_ = true;
	return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
