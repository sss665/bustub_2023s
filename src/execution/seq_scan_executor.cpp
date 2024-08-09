//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
	: AbstractExecutor(exec_ctx),
		plan_(plan),
		table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())),
		it_(std::make_unique<TableIterator>(table_info_->table_->MakeIterator())){}

void SeqScanExecutor::Init() {
	it_ = std::make_unique<TableIterator>(table_info_->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
	if(it_->IsEnd()){
		return false;
	}
	auto tupleMeta = it_->GetTuple().first;
	while (tupleMeta.is_deleted_){
		++(*it_);
		if(it_->IsEnd()){
			return false;
		}
		tupleMeta = it_->GetTuple().first;
	}
	*tuple = it_->GetTuple().second;
	*rid = it_->GetRID();
	++(*it_);
	return true;
}

}  // namespace bustub
