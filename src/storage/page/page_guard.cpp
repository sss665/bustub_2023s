#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
}

void BasicPageGuard::Drop() {
  if (page_ == nullptr) {
    return;
  }
  bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  page_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  Drop();
  bpm_ = that.bpm_;
  page_ = that.page_;
  is_dirty_ = that.is_dirty_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  already_unlock_ = that.already_unlock_;
  that.already_unlock_ = true;
  guard_ = std::move(that.guard_);
};

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  Drop();
  already_unlock_ = that.already_unlock_;
  that.already_unlock_ = true;
  guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ != nullptr && !already_unlock_) {
    // std::cout<<PageId() << "RUnlatch"<<std::endl;
    guard_.page_->RUnlatch();
    already_unlock_ = true;
    guard_.Drop();
  }
}

ReadPageGuard::~ReadPageGuard() { Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  already_unlock_ = that.already_unlock_;
  that.already_unlock_ = true;
  guard_ = std::move(that.guard_);
};

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  Drop();
  already_unlock_ = that.already_unlock_;
  that.already_unlock_ = true;
  guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ != nullptr && !already_unlock_) {
    // std::cout<<PageId() << "WUnlatch"<<std::endl;
    guard_.page_->WUnlatch();
    already_unlock_ = true;
    guard_.Drop();
  }
}

WritePageGuard::~WritePageGuard() { Drop(); }  // NOLINT

}  // namespace bustub
