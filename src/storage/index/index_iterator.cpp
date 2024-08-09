/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t page_id, int num, BufferPoolManager *bpm) {
  page_id_ = page_id;
  num_ = num;
  bpm_ = bpm;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return page_id_ == INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  WritePageGuard guard = bpm_->FetchPageWrite(page_id_);
  auto leaf = guard.As<LeafPage>();
  // MappingType& mp = leaf->PairAt(num_);
  return leaf->PairAt(num_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  ReadPageGuard guard = bpm_->FetchPageRead(page_id_);
  auto leaf = guard.As<LeafPage>();
  if (num_ != leaf->GetSize() - 1) {
    num_++;
  } else {
    if (leaf->GetNextPageId() == INVALID_PAGE_ID) {
      bpm_ = nullptr;
      page_id_ = INVALID_PAGE_ID;
      num_ = -1;
    } else {
      page_id_ = leaf->GetNextPageId();
      num_ = 0;
    }
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
