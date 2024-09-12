//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <set>

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {
auto LockManager::AreLocksCompatible(LockMode l1, LockMode l2) -> bool {
  switch (l1) {
    case LockMode::SHARED:
      return l2 == LockMode::INTENTION_SHARED || l2 == LockMode::SHARED;
    case LockMode::EXCLUSIVE:
      return false;
    case LockMode::INTENTION_SHARED:
      return l2 != LockMode::EXCLUSIVE;

    case LockMode::INTENTION_EXCLUSIVE:
      return l2 == LockMode::INTENTION_SHARED || l2 == LockMode::INTENTION_EXCLUSIVE;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return l2 == LockMode::INTENTION_SHARED;
  }
  return true;
}
auto LockManager::AreLocksUpgrade(LockMode l1, LockMode l2) -> bool {
  switch (l1) {
    case LockMode::SHARED:
      return l2 == LockMode::EXCLUSIVE || l2 == LockMode::SHARED_INTENTION_EXCLUSIVE;
    case LockMode::EXCLUSIVE:
      return false;
    case LockMode::INTENTION_SHARED:
      return true;
    case LockMode::INTENTION_EXCLUSIVE:
      return l2 == LockMode::EXCLUSIVE || l2 == LockMode::SHARED_INTENTION_EXCLUSIVE;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return l2 == LockMode::EXCLUSIVE;
  }
  return true;
}
auto LockManager::CanGetLock(std::shared_ptr<LockRequestQueue> &lock_request_queue, Transaction *txn,
                             LockMode lock_mode) -> bool {
  if (txn->GetState() == TransactionState::ABORTED) {
    return true;
  }
  for (auto it = lock_request_queue->request_queue_.begin(); (*it)->granted_; it++) {
    if ((*it)->txn_id_ == txn->GetTransactionId()) {
      continue;
    }
    if (!AreLocksCompatible((*it)->lock_mode_, lock_mode)) {
      return false;
    }
  }
  return true;
}
auto LockManager::GetTxnSet(Transaction *txn, LockMode lock_mode) -> std::shared_ptr<std::unordered_set<table_oid_t>> {
  switch (lock_mode) {
    case LockMode::SHARED:
      return txn->GetSharedTableLockSet();
    case LockMode::EXCLUSIVE:
      return txn->GetExclusiveTableLockSet();
    case LockMode::INTENTION_SHARED:
      return txn->GetIntentionSharedTableLockSet();
    case LockMode::INTENTION_EXCLUSIVE:
      return txn->GetIntentionExclusiveTableLockSet();
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return txn->GetSharedIntentionExclusiveTableLockSet();
  }
  return nullptr;
}
auto LockManager::GetTxnRowSet(Transaction *txn, LockMode lock_mode)
    -> std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> {
  if (lock_mode == LockMode::EXCLUSIVE) {
    return txn->GetExclusiveRowLockSet();
  }
  if (lock_mode == LockMode::SHARED) {
    return txn->GetSharedRowLockSet();
  }
  return nullptr;
}

auto LockManager::UpgradeLockTable(std::shared_ptr<LockRequestQueue> &lock_request_queue, Transaction *txn,
                                   LockMode lock_mode, const table_oid_t &oid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED) {
    return true;
  }
  if (!CanGetLock(lock_request_queue, txn, lock_mode)) {
    return false;
  }
  for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end(); it++) {
    if ((*it)->txn_id_ == txn->GetTransactionId() && !(*it)->granted_) {
      (*it)->granted_ = true;
      std::shared_ptr<std::unordered_set<table_oid_t>> lock_mode_set = GetTxnSet(txn, lock_mode);
      lock_mode_set->insert(oid);
    }
  }
  lock_request_queue->upgrading_ = INVALID_TXN_ID;
  return true;
}

auto LockManager::IsolationCheck(Transaction *txn, LockMode lock_mode) -> void {
  if ((lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE ||
       lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) &&
      txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED:
      if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
          lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      break;
    case IsolationLevel::REPEATABLE_READ:
      if (txn->GetState() == TransactionState::SHRINKING) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      break;
  }
}
auto LockManager::PrintLockMode(LockMode lockmode) -> void {
  switch (lockmode) {
    case LockMode::SHARED:
      std::cout << "SHARED" << std::endl;
      break;
    case LockMode::EXCLUSIVE:
      std::cout << "EXCLUSIVE" << std::endl;
      break;
    case LockMode::INTENTION_SHARED:
      std::cout << "INTENTION_SHARED" << std::endl;
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      std::cout << "INTENTION_EXCLUSIVE" << std::endl;
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      std::cout << "SHARED_INTENTION_EXCLUSIVE" << std::endl;
      break;
    default:
      std::cout << "Unknown LockMode" << std::endl;
      break;
  }
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  std::cout << "locktable:  " << txn->GetTransactionId() << std::endl;
  PrintLockMode(lock_mode);
  std::cout << " " << oid << std::endl;
  IsolationCheck(txn, lock_mode);
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  std::shared_ptr<LockRequestQueue> lock_request_queue = table_lock_map_[oid];
  table_lock_map_latch_.unlock();
  std::unique_lock<std::mutex> lk(lock_request_queue->latch_);
  auto pos = lock_request_queue->request_queue_.end();
  for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end(); it++) {
    if ((*it)->txn_id_ == txn->GetTransactionId() && (*it)->granted_) {
      if (lock_mode == (*it)->lock_mode_) {
        return true;
      }
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      if (!AreLocksUpgrade((*it)->lock_mode_, lock_mode)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      lock_request_queue->upgrading_ = txn->GetTransactionId();
      break;
    }
  }
  if (lock_request_queue->upgrading_ == txn->GetTransactionId()) {
    auto pos1 = lock_request_queue->request_queue_.end();
    for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end(); it++) {
      if (!(*it)->granted_) {
        pos1 = it;
        break;
      }
    }
    auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
    lock_request_queue->request_queue_.insert(pos1, lock_request);
    for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end(); it++) {
      if ((*it)->txn_id_ == txn->GetTransactionId() && (*it)->granted_) {
        std::shared_ptr<std::unordered_set<table_oid_t>> lock_mode_set = GetTxnSet(txn, (*it)->lock_mode_);
        lock_mode_set->erase(oid);
        lock_request_queue->request_queue_.erase(it);
        break;
      }
    }
    lock_request_queue->cv_.wait(lk, [&] { return UpgradeLockTable(lock_request_queue, txn, lock_mode, oid); });
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
    lock_request_queue->cv_.notify_all();
    return true;
  }
  auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  lock_request_queue->request_queue_.push_back(lock_request);
  pos = --lock_request_queue->request_queue_.end();
  if (lock_request_queue->request_queue_.size() == 1 || (*--pos)->granted_) {
    lock_request_queue->cv_.wait(lk, [&]() { return CanGetLock(lock_request_queue, txn, lock_mode); });
  } else {
    lock_request_queue->cv_.wait(lk, [&]() -> bool {
      if (txn->GetState() == TransactionState::ABORTED) {
        return true;
      }
      for (auto &it : lock_request_queue->request_queue_) {
        if (txn->GetTransactionId() != it->txn_id_ && !it->granted_) {
          return false;
        }
        if (txn->GetTransactionId() == it->txn_id_) {
          break;
        }
      }
      return CanGetLock(lock_request_queue, txn, lock_mode);
    });
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    lock_request_queue->request_queue_.remove(lock_request);
    lock_request_queue->cv_.notify_all();
    return false;
  }
  for (auto &it : lock_request_queue->request_queue_) {
    if (txn->GetTransactionId() == it->txn_id_) {
      it->granted_ = true;
      break;
    }
  }
  std::shared_ptr<std::unordered_set<table_oid_t>> lock_mode_set = GetTxnSet(txn, lock_mode);
  lock_mode_set->insert(oid);
  lock_request_queue->cv_.notify_all();
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  std::cout << "Unlocktable:  " << txn->GetTransactionId() << " " << oid << std::endl;
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  std::shared_ptr<LockRequestQueue> lock_request_queue = table_lock_map_[oid];
  table_lock_map_latch_.unlock();
  std::unique_lock<std::mutex> lk(lock_request_queue->latch_);
  bool has_locked = false;
  bool row_locked = false;
  LockMode lock_mode;
  auto pos = lock_request_queue->request_queue_.end();
  for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end(); it++) {
    if ((*it)->txn_id_ == txn->GetTransactionId() && (*it)->granted_) {
      has_locked = true;
      lock_mode = (*it)->lock_mode_;
      pos = it;
      break;
    }
  }
  if (!has_locked) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> s_row_lock_set = txn->GetSharedRowLockSet();
  if (s_row_lock_set->find(oid) != s_row_lock_set->end() && !s_row_lock_set->at(oid).empty()) {
    row_locked = true;
  }
  std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> x_row_lock_set =
      txn->GetExclusiveRowLockSet();
  if (x_row_lock_set->find(oid) != x_row_lock_set->end() && !x_row_lock_set->at(oid).empty()) {
    row_locked = true;
  }
  if (row_locked) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  if (lock_mode == LockMode::EXCLUSIVE) {
    txn->SetState(TransactionState::SHRINKING);
  }
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED:
      break;
    case IsolationLevel::REPEATABLE_READ:
      if (lock_mode == LockMode::SHARED) {
        txn->SetState(TransactionState::SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      break;
  }

  std::shared_ptr<std::unordered_set<table_oid_t>> lock_mode_set = GetTxnSet(txn, lock_mode);
  lock_mode_set->erase(oid);
  lock_request_queue->request_queue_.erase(pos);
  lock_request_queue->cv_.notify_all();
  return true;
}

auto LockManager::CheckAppropriateLockOnTable(Transaction *txn, const table_oid_t &oid, LockMode row_lock_mode)
    -> void {
  bool right = false;
  std::shared_ptr<std::unordered_set<table_oid_t>> lock_set;
  if (row_lock_mode == LockMode::SHARED) {
    lock_set = txn->GetSharedTableLockSet();
    if (lock_set->find(oid) != lock_set->end()) {
      right = true;
    }
    lock_set = txn->GetIntentionSharedTableLockSet();
    if (lock_set->find(oid) != lock_set->end()) {
      right = true;
    }
    lock_set = txn->GetSharedIntentionExclusiveTableLockSet();
    if (lock_set->find(oid) != lock_set->end()) {
      right = true;
    }
    lock_set = txn->GetIntentionExclusiveTableLockSet();
    if (lock_set->find(oid) != lock_set->end()) {
      right = true;
    }
    lock_set = txn->GetExclusiveTableLockSet();
    if (lock_set->find(oid) != lock_set->end()) {
      right = true;
    }
    if (!right) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }
  right = false;
  if (row_lock_mode == LockMode::EXCLUSIVE) {
    lock_set = txn->GetExclusiveTableLockSet();
    if (lock_set->find(oid) != lock_set->end()) {
      right = true;
    }
    lock_set = txn->GetIntentionExclusiveTableLockSet();
    if (lock_set->find(oid) != lock_set->end()) {
      right = true;
    }
    lock_set = txn->GetSharedIntentionExclusiveTableLockSet();
    if (lock_set->find(oid) != lock_set->end()) {
      right = true;
    }
    if (!right) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    }
  }
}

auto LockManager::UpgradeLockRow(std::shared_ptr<LockRequestQueue> &lock_request_queue, Transaction *txn,
                                 LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  if (txn->GetState() == TransactionState::ABORTED) {
    return true;
  }
  if (!CanGetLock(lock_request_queue, txn, lock_mode)) {
    return false;
  }
  for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end(); it++) {
    if ((*it)->txn_id_ == txn->GetTransactionId() && !(*it)->granted_) {
      (*it)->granted_ = true;
      std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> lock_mode_set =
          GetTxnRowSet(txn, lock_mode);
      (*lock_mode_set)[oid].insert(rid);
    }
  }
  lock_request_queue->upgrading_ = INVALID_TXN_ID;
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  std::cout << "lockrow:  " << txn->GetTransactionId() << std::endl;
  PrintLockMode(lock_mode);
  std::cout << " " << oid << " " << rid.ToString() << std::endl;
  if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE || lock_mode == LockMode::INTENTION_SHARED ||
      lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  IsolationCheck(txn, lock_mode);
  CheckAppropriateLockOnTable(txn, oid, lock_mode);
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  std::shared_ptr<LockRequestQueue> lock_request_queue = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  std::unique_lock<std::mutex> lk(lock_request_queue->latch_);
  auto pos = lock_request_queue->request_queue_.end();
  for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end(); it++) {
    if ((*it)->txn_id_ == txn->GetTransactionId() && (*it)->granted_) {
      if (lock_mode == (*it)->lock_mode_) {
        return true;
      }
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      if (!AreLocksUpgrade((*it)->lock_mode_, lock_mode)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      }
      lock_request_queue->upgrading_ = txn->GetTransactionId();
      break;
    }
  }
  if (lock_request_queue->upgrading_ == txn->GetTransactionId()) {
    auto pos1 = lock_request_queue->request_queue_.end();
    for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end(); it++) {
      if (!(*it)->granted_) {
        pos1 = it;
        break;
      }
    }
    auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
    lock_request_queue->request_queue_.insert(pos1, lock_request);
    for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end(); it++) {
      if ((*it)->txn_id_ == txn->GetTransactionId() && (*it)->granted_) {
        std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> lock_mode_set =
            GetTxnRowSet(txn, (*it)->lock_mode_);
        (*lock_mode_set)[oid].erase(rid);
        lock_request_queue->request_queue_.erase(it);
        break;
      }
    }
    lock_request_queue->cv_.wait(lk, [&] { return UpgradeLockRow(lock_request_queue, txn, lock_mode, oid, rid); });
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
    lock_request_queue->cv_.notify_all();
    return true;
  }
  auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  lock_request_queue->request_queue_.push_back(lock_request);
  pos = --lock_request_queue->request_queue_.end();
  if (lock_request_queue->request_queue_.size() == 1 || (*--pos)->granted_) {
    lock_request_queue->cv_.wait(lk, [&]() { return CanGetLock(lock_request_queue, txn, lock_mode); });
  } else {
    lock_request_queue->cv_.wait(lk, [&]() -> bool {
      if (txn->GetState() == TransactionState::ABORTED) {
        return true;
      }
      for (auto &it : lock_request_queue->request_queue_) {
        if (txn->GetTransactionId() != it->txn_id_ && !it->granted_) {
          return false;
        }
        if (txn->GetTransactionId() == it->txn_id_) {
          break;
        }
      }
      return CanGetLock(lock_request_queue, txn, lock_mode);
    });
  }
  if (txn->GetState() == TransactionState::ABORTED) {
    lock_request_queue->request_queue_.remove(lock_request);
    lock_request_queue->cv_.notify_all();
    return false;
  }
  for (auto &it : lock_request_queue->request_queue_) {
    if (txn->GetTransactionId() == it->txn_id_) {
      it->granted_ = true;
      break;
    }
  }
  std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> lock_mode_set =
      GetTxnRowSet(txn, lock_mode);
  (*lock_mode_set)[oid].insert(rid);
  lock_request_queue->cv_.notify_all();
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  std::cout << "unlockrow:  " << txn->GetTransactionId() << " " << oid << " " << rid.ToString() << std::endl;
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }
  std::shared_ptr<LockRequestQueue> lock_request_queue = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  std::unique_lock<std::mutex> lk(lock_request_queue->latch_);
  bool has_locked = false;
  LockMode lock_mode;
  auto pos = lock_request_queue->request_queue_.end();
  for (auto it = lock_request_queue->request_queue_.begin(); it != lock_request_queue->request_queue_.end(); it++) {
    if ((*it)->txn_id_ == txn->GetTransactionId() && (*it)->granted_) {
      has_locked = true;
      lock_mode = (*it)->lock_mode_;
      pos = it;
      break;
    }
  }
  if (!has_locked) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  if (!force) {
    if (lock_mode == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::SHRINKING);
    }
    switch (txn->GetIsolationLevel()) {
      case IsolationLevel::READ_UNCOMMITTED:
        break;
      case IsolationLevel::REPEATABLE_READ:
        if (lock_mode == LockMode::SHARED) {
          txn->SetState(TransactionState::SHRINKING);
        }
        break;
      case IsolationLevel::READ_COMMITTED:
        break;
    }
  }

  std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> lock_mode_set =
      GetTxnRowSet(txn, lock_mode);
  (*lock_mode_set)[oid].erase(rid);
  lock_request_queue->request_queue_.erase(pos);
  lock_request_queue->cv_.notify_all();
  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  std::unique_lock<std::mutex> lk(waits_for_latch_);
  std::vector<txn_id_t> &vec = waits_for_[t1];
  auto it = std::find(vec.begin(), vec.end(), t2);
  if (it == vec.end()) {
    vec.push_back(t2);
  }
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  std::unique_lock<std::mutex> lk(waits_for_latch_);
  for (auto it = waits_for_[t1].begin(); it != waits_for_[t1].end(); it++) {
    if (*it == t2) {
      waits_for_[t1].erase(it);
      break;
    }
  }
}

auto LockManager::FindCycle(txn_id_t source_txn, std::vector<txn_id_t> &path, std::unordered_set<txn_id_t> &visited,
                            txn_id_t *abort_txn_id) -> bool {
  if (visited.find(source_txn) != visited.end()) {
    bool v = false;
    txn_id_t max1 = 0;
    for (auto &txn_id : path) {
      if (txn_id == source_txn) {
        v = true;
      }
      if (v) {
        if (txn_id > max1) {
          max1 = txn_id;
        }
      }
    }
    std::cout << max1 << std::endl;
    *abort_txn_id = max1;
    return true;
  }
  visited.insert(source_txn);
  path.push_back(source_txn);
  waits_for_latch_.lock();
  std::set<txn_id_t> sort_waits_for(waits_for_[source_txn].begin(), waits_for_[source_txn].end());
  waits_for_latch_.unlock();
  for (txn_id_t txn_id : sort_waits_for) {
    if (FindCycle(txn_id, path, visited, abort_txn_id)) {
      return true;
    }
  }
  path.pop_back();
  return false;
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  std::set<txn_id_t> keys;
  // 使用迭代器遍历 unordered_map 并提取键
  waits_for_latch_.lock();
  for (const auto &pair : waits_for_) {
    keys.insert(pair.first);  // 插入 key
  }
  waits_for_latch_.unlock();
  for (auto &key : keys) {
    waits_for_latch_.lock();
    if (waits_for_[key].empty()) {
      waits_for_latch_.unlock();
      continue;
    }
    waits_for_latch_.unlock();
    std::vector<txn_id_t> path;
    std::unordered_set<txn_id_t> on_path;
    std::unordered_set<txn_id_t> visited;
    txn_id_t abort_txn_id;
    if (FindCycle(key, path, visited, &abort_txn_id)) {
      *txn_id = abort_txn_id;
      return true;
    }
  }
  return false;
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  waits_for_latch_.lock();
  for (auto &pa : waits_for_) {
    for (auto &txn_id : pa.second) {
      edges.emplace_back(pa.first, txn_id);
    }
  }
  waits_for_latch_.unlock();
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      table_lock_map_latch_.lock();
      for (auto &pa : table_lock_map_) {
        std::shared_ptr<LockRequestQueue> lock_request_queue = pa.second;
        lock_request_queue->latch_.lock();
        std::unordered_set<txn_id_t> grant_txn_set;
        for (auto &lock_req : lock_request_queue->request_queue_) {
          Transaction *txn = txn_manager_->GetTransaction(lock_req->txn_id_);
          if (txn->GetState() == TransactionState::ABORTED) {
            continue;
          }
          if (lock_req->granted_) {
            grant_txn_set.insert(lock_req->txn_id_);
          } else {
            for (auto &txn_id : grant_txn_set) {
              waits_table_latch_.lock();
              waits_table_[lock_req->txn_id_].push_back(lock_req->oid_);
              waits_table_latch_.unlock();
              AddEdge(lock_req->txn_id_, txn_id);
            }
          }
        }
        lock_request_queue->latch_.unlock();
      }
      table_lock_map_latch_.unlock();
      row_lock_map_latch_.lock();
      for (auto &pa : row_lock_map_) {
        std::shared_ptr<LockRequestQueue> lock_request_queue = pa.second;
        lock_request_queue->latch_.lock();
        std::unordered_set<txn_id_t> grant_txn_set;
        for (auto &lock_req : lock_request_queue->request_queue_) {
          Transaction *txn = txn_manager_->GetTransaction(lock_req->txn_id_);
          if (txn->GetState() == TransactionState::ABORTED) {
            continue;
          }
          if (lock_req->granted_) {
            grant_txn_set.insert(lock_req->txn_id_);
          } else {
            for (auto &txn_id : grant_txn_set) {
              waits_rid_latch_.lock();
              waits_rid_[lock_req->txn_id_].push_back(lock_req->rid_);
              waits_rid_latch_.unlock();
              AddEdge(lock_req->txn_id_, txn_id);
            }
          }
        }
        lock_request_queue->latch_.unlock();
      }
      row_lock_map_latch_.unlock();
      txn_id_t txn_id;
      while (HasCycle(&txn_id)) {
        std::cout << txn_id << std::endl;
        Transaction *txn = txn_manager_->GetTransaction(txn_id);
        txn_manager_->Abort(txn);
        waits_for_latch_.lock();
        for (auto &pa : waits_for_) {
          waits_for_latch_.unlock();
          RemoveEdge(pa.first, txn_id);
          waits_for_latch_.lock();
        }
        waits_for_latch_.unlock();
        for (table_oid_t &toid : waits_table_[txn_id]) {
          table_lock_map_latch_.lock();
          table_lock_map_[toid]->cv_.notify_all();
          table_lock_map_latch_.unlock();
        }
        for (RID &rid : waits_rid_[txn_id]) {
          row_lock_map_latch_.lock();
          row_lock_map_[rid]->cv_.notify_all();
          row_lock_map_latch_.unlock();
        }
        waits_for_latch_.lock();
        waits_for_.erase(txn_id);
        waits_for_latch_.unlock();
      }
      waits_for_latch_.lock();
      waits_for_.clear();
      waits_for_latch_.unlock();
    }
  }
}

}  // namespace bustub
