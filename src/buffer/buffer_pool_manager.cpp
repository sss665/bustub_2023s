//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::unique_lock<std::mutex> lock(latch_);
  frame_id_t fi;
  if (!free_list_.empty()) {
    fi = free_list_.front();
    free_list_.pop_front();
    *page_id = AllocatePage();
    page_table_[*page_id] = fi;
    pages_[fi].page_id_ = *page_id;
    replacer_->RecordAccess(fi);
    replacer_->SetEvictable(fi, false);
    pages_[fi].pin_count_ = 1;
    return &pages_[fi];
  }
  bool has_page = replacer_->Evict(&fi);
  if (!has_page) {
    return nullptr;
  }
  if (pages_[fi].IsDirty()) {
    disk_manager_->WritePage(pages_[fi].page_id_, pages_[fi].data_);
    pages_[fi].is_dirty_ = false;
  }
  page_table_.erase(pages_[fi].page_id_);
  *page_id = AllocatePage();
  page_table_[*page_id] = fi;
  pages_[fi].page_id_ = *page_id;
  memset(pages_[fi].data_, '1', BUSTUB_PAGE_SIZE);
  replacer_->RecordAccess(fi);
  replacer_->SetEvictable(fi, false);
  pages_[fi].pin_count_ = 1;
  return &pages_[fi];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::unique_lock<std::mutex> lock(latch_);
  frame_id_t fi;
  if (page_table_.find(page_id) != page_table_.end()) {
    fi = page_table_[page_id];
    replacer_->RecordAccess(fi);
    replacer_->SetEvictable(fi, false);
    pages_[fi].pin_count_++;
    return &pages_[fi];
  }

  if (!free_list_.empty()) {
    fi = free_list_.front();
    free_list_.pop_front();
    page_table_[page_id] = fi;
    pages_[fi].page_id_ = page_id;
    disk_manager_->ReadPage(page_id, pages_[fi].data_);
    replacer_->RecordAccess(fi);
    replacer_->SetEvictable(fi, false);
    pages_[fi].pin_count_ = 1;
    return &pages_[fi];
  }
  bool has_page = replacer_->Evict(&fi);
  if (!has_page) {
    return nullptr;
  }

  if (pages_[fi].IsDirty()) {
    disk_manager_->WritePage(pages_[fi].page_id_, pages_[fi].data_);
    pages_[fi].is_dirty_ = false;
  }
  page_table_.erase(pages_[fi].page_id_);
  pages_[fi].page_id_ = page_id;
  pages_[fi].pin_count_ = 1;
  page_table_[page_id] = fi;
  memset(pages_[fi].data_, '2', BUSTUB_PAGE_SIZE);
  disk_manager_->ReadPage(page_id, pages_[fi].data_);
  replacer_->RecordAccess(fi);
  replacer_->SetEvictable(fi, false);
  return &pages_[fi];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::unique_lock<std::mutex> lock(latch_);
  frame_id_t fi;
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  fi = page_table_[page_id];

  if (pages_[fi].pin_count_ == 0) {
    return false;
  }
  pages_[fi].is_dirty_ |= is_dirty;
  if (--pages_[fi].pin_count_ == 0) {
    replacer_->SetEvictable(fi, true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> lock(latch_);

  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  frame_id_t fi = page_table_[page_id];
  disk_manager_->WritePage(page_id, pages_[fi].data_);
  pages_[fi].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (auto &p : page_table_) {
    FlushPage(p.first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> lock(latch_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  frame_id_t fi = page_table_[page_id];
  if (pages_[fi].GetPinCount() > 0) {
    return false;
  }
  if (pages_[fi].IsDirty()) {
    disk_manager_->WritePage(page_id, pages_[fi].data_);
    pages_[fi].is_dirty_ = false;
  }
  page_table_.erase(page_id);
  replacer_->Remove(fi);
  free_list_.emplace_back(static_cast<int>(fi));
  pages_[fi].pin_count_ = 0;
  pages_[fi].page_id_ = INVALID_PAGE_ID;
  memset(pages_[fi].data_, '1', BUSTUB_PAGE_SIZE);
  pages_[fi].is_dirty_ = false;
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto fetch_page = FetchPage(page_id);
  // std::cout<<page_id << "wants RLatch"<<std::endl;
  fetch_page->RLatch();
  // std::cout<<page_id << "gets RLatch"<<std::endl;
  return {this, fetch_page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto fetch_page = FetchPage(page_id);
  // std::cout<<page_id << "wants WLatch"<<std::endl;
  fetch_page->WLatch();
  // std::cout<<page_id << "gets WLatch"<<std::endl;
  return {this, fetch_page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

}  // namespace bustub
