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
      it_(std::make_unique<TableIterator>(table_info_->table_->MakeEagerIterator())),
      txn_(exec_ctx->GetTransaction()) {}

void SeqScanExecutor::Init() {
  if (exec_ctx_->IsDelete()) {
    exec_ctx_->GetLockManager()->LockTable(txn_, LockManager::LockMode::INTENTION_EXCLUSIVE, table_info_->oid_);
  } else {
    if (txn_->GetIsolationLevel() == IsolationLevel::READ_COMMITTED ||
        txn_->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      if (txn_->GetIntentionExclusiveTableLockSet()->find(table_info_->oid_) ==
          txn_->GetIntentionExclusiveTableLockSet()->end()) {
        exec_ctx_->GetLockManager()->LockTable(txn_, LockManager::LockMode::INTENTION_SHARED, table_info_->oid_);
      }
    }
  }
  it_ = std::make_unique<TableIterator>(table_info_->table_->MakeEagerIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (it_->IsEnd()) {
    if (!exec_ctx_->IsDelete() && txn_->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      if (txn_->GetIntentionSharedTableLockSet()->find(table_info_->oid_) !=
          txn_->GetIntentionSharedTableLockSet()->end()) {
        exec_ctx_->GetLockManager()->UnlockTable(txn_, table_info_->oid_);
      }
    }
    return false;
  }
  if (exec_ctx_->IsDelete()) {
    exec_ctx_->GetLockManager()->LockRow(txn_, LockManager::LockMode::EXCLUSIVE, table_info_->oid_, it_->GetRID());
  } else {
    if (txn_->GetIsolationLevel() == IsolationLevel::READ_COMMITTED ||
        txn_->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      if (txn_->GetExclusiveRowLockSet()->find(table_info_->oid_) == txn_->GetExclusiveRowLockSet()->end()) {
        exec_ctx_->GetLockManager()->LockRow(txn_, LockManager::LockMode::SHARED, table_info_->oid_, it_->GetRID());
      }
    }
  }
  auto tuple_meta = it_->GetTuple().first;
  while (tuple_meta.is_deleted_) {
    if (!(txn_->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED && !exec_ctx_->IsDelete())) {
      exec_ctx_->GetLockManager()->UnlockRow(txn_, table_info_->oid_, it_->GetRID(), true);
    }
    ++(*it_);
    if (it_->IsEnd()) {
      if (!exec_ctx_->IsDelete() && txn_->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
        if (txn_->GetIntentionSharedTableLockSet()->find(table_info_->oid_) !=
            txn_->GetIntentionSharedTableLockSet()->end()) {
          exec_ctx_->GetLockManager()->UnlockTable(txn_, table_info_->oid_);
        }
      }
      return false;
    }
    if (exec_ctx_->IsDelete()) {
      exec_ctx_->GetLockManager()->LockRow(txn_, LockManager::LockMode::EXCLUSIVE, table_info_->oid_, it_->GetRID());
    } else {
      if (txn_->GetIsolationLevel() == IsolationLevel::READ_COMMITTED ||
          txn_->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
        if (txn_->GetExclusiveRowLockSet()->find(table_info_->oid_) == txn_->GetExclusiveRowLockSet()->end()) {
          exec_ctx_->GetLockManager()->LockRow(txn_, LockManager::LockMode::SHARED, table_info_->oid_, it_->GetRID());
        }
      }
    }
    tuple_meta = it_->GetTuple().first;
  }
  *tuple = it_->GetTuple().second;
  *rid = it_->GetRID();
  if (!exec_ctx_->IsDelete() && txn_->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    if ((*txn_->GetSharedRowLockSet())[table_info_->oid_].find(it_->GetRID()) !=
        (*txn_->GetSharedRowLockSet())[table_info_->oid_].end()) {
      exec_ctx_->GetLockManager()->UnlockRow(txn_, table_info_->oid_, it_->GetRID());
    }
  }
  ++(*it_);
  return true;
}

}  // namespace bustub
