//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
		plan_(plan),
		index_info_(exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid())),
		tbl_heap_(exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_)->table_.get()),
		tree_(dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get())),
		it_(tree_->GetBeginIterator()){}

void IndexScanExecutor::Init() {
	it_ = tree_->GetBeginIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
	if(it_.IsEnd()){
		return false;
	}
	auto currentRid = (*it_).second;
	auto tupleMeta = tbl_heap_->GetTuple(currentRid).first;
	while (tupleMeta.is_deleted_){
		++it_;
		if(it_.IsEnd()){
			return false;
		}
		currentRid = (*it_).second;
		tupleMeta = tbl_heap_->GetTuple(currentRid).first;
	}
	*tuple = tbl_heap_->GetTuple(currentRid).second;
	*rid = currentRid;
	++it_;
	return true;
}

}  // namespace bustub
