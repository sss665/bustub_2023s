//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) ,
		plan_(plan),
		table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())),
		table_index_(exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)),
		child_executor_(std::move(child_executor)),
		done_(false){}

void InsertExecutor::Init() {
	child_executor_->Init();
	done_ = false;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
	// auto filter_expr = plan_->GetPredicate();
	// Get the next tuple
	if (done_) {
		return false;
	}
	int32_t col = 0;
	TupleMeta insertTupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, false};
	Tuple insertTuple;
	RID commonRid;
	while (true) {
		// Get the next tuple
		const auto status = child_executor_->Next(&insertTuple, &commonRid);
		if (!status) {
			break;
		}
		auto insertRid = table_info_->table_->InsertTuple(insertTupleMeta, insertTuple,exec_ctx_->GetLockManager(),exec_ctx_->GetTransaction(),table_info_->oid_);
		for (const auto &indexs : table_index_) {
			indexs->index_->InsertEntry(insertTuple.KeyFromTuple(table_info_->schema_, indexs->key_schema_,indexs->index_->GetKeyAttrs()), *insertRid, exec_ctx_->GetTransaction());
		}
		col++;
	}
	done_ = true;
	*tuple = Tuple({Value(INTEGER,col)}, &GetOutputSchema());
	return true;
}

}  // namespace bustub
