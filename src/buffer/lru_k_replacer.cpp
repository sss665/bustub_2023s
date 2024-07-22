//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::unique_lock<std::mutex> lock(latch_);
  size_t min = 0x3f3f3f3f;
  auto tmp = less_k_.end();
  for (auto it = less_k_.begin(); it != less_k_.end(); it++) {
    if (node_store_.at(*it).GetEvictable()) {
      size_t val = node_store_.at(*it).GetTime();
      if (val < min) {
        min = val;
        tmp = it;
      }
    }
  }
  if (tmp != less_k_.end()) {
    *frame_id = *tmp;
    node_store_.erase(*tmp);
    less_k_.erase(tmp);
    return true;
  }
  min = 0x3f3f3f3f;
  tmp = sat_k_.end();
  for (auto it = sat_k_.begin(); it != sat_k_.end(); it++) {
    if (node_store_.at(*it).GetEvictable()) {
      size_t val = node_store_.at(*it).GetTime();
      if (val < min) {
        min = val;
        tmp = it;
      }
    }
  }
  if (tmp != sat_k_.end()) {
    *frame_id = *tmp;
    node_store_.erase(*tmp);
    sat_k_.erase(tmp);
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::unique_lock<std::mutex> lock(latch_);
  bool sat;
  if (node_store_.find(frame_id) == node_store_.end()) {
    LRUKNode ln(frame_id, k_);
    sat = ln.AddHistory(current_timestamp_);
    node_store_.insert(std::pair<frame_id_t, LRUKNode>(frame_id, ln));
    less_k_.push_back(frame_id);
  } else {
    sat = node_store_.at(frame_id).AddHistory(current_timestamp_);
  }
  if (sat) {
    auto it = std::find(less_k_.begin(), less_k_.end(), frame_id);
    if (it != less_k_.end()) {
      sat_k_.push_back(*it);
      less_k_.erase(it);
    }
  }
  current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::unique_lock<std::mutex> lock(latch_);
  if (node_store_.find(frame_id) != node_store_.end()) {
    node_store_.at(frame_id).SetEvictable(set_evictable);
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::unique_lock<std::mutex> lock(latch_);
  node_store_.erase(frame_id);
  for (auto it = less_k_.begin(); it != less_k_.end(); it++) {
    if (*it == frame_id) {
      less_k_.erase(it);
      return;
    }
  }
  for (auto it = sat_k_.begin(); it != sat_k_.end(); it++) {
    if (*it == frame_id) {
      sat_k_.erase(it);
      return;
    }
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::unique_lock<std::mutex> lock(latch_);
  size_t sum = 0;
  for (int &it : less_k_) {
    if (node_store_.at(it).GetEvictable()) {
      sum++;
    }
  }
  for (int &it : sat_k_) {
    if (node_store_.at(it).GetEvictable()) {
      sum++;
    }
  }

  return sum;
}

}  // namespace bustub
