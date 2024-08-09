//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }
/*
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetPrePageId() const -> page_id_t {
  return pre_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetPrePageId(page_id_t pre_page_id) {
  pre_page_id_ = pre_page_id;
}
*/
/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  CheckLegal(index, " leaf keyat ");
  KeyType key = array_[index].first;
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  CheckLegal(index, " leaf valueat ");
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::FindValue(const KeyType &key, const KeyComparator &comparator,
                                           std::vector<ValueType> *result) const -> bool {
  int l = 0;
  int r = GetSize() - 1;
  while (l <= r) {
    int mid = (l + r) / 2;
    int res = comparator(key, KeyAt(mid));
    if (res == 0) {
      (*result).push_back(ValueAt(mid));
      return true;
    }
    if (res < 0) {
      r = mid - 1;
    } else {
      l = mid + 1;
    }
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> bool {
  int l = 0;
  int r = GetSize() - 1;
  while (l <= r) {
    int mid = (l + r) / 2;
    int res = comparator(key, KeyAt(mid));
    if (res == 0) {
      return false;
    }
    if (res < 0) {
      r = mid - 1;
    } else {
      l = mid + 1;
    }
  }
  for (int j = GetSize() - 1; j >= l; j--) {
    array_[j + 1] = array_[j];
  }
  CheckLegalInsert(l, " leaf insert key ");
  array_[l] = {key, value};
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(int index, MappingType mp) {
  CheckLegalInsert(index, " leaf insert index");
  for (int i = GetSize() - 1; i >= index; i--) {
    array_[i + 1] = array_[i];
  }
  array_[index] = mp;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyOut(std::shared_ptr<MappingType[]> &tmp) {
  int max_size = GetMaxSize();
  for (int i = max_size / 2; i < max_size; i++) {
    tmp[i - max_size / 2] = array_[i];
  }
  SetSize(max_size / 2);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyIn(std::shared_ptr<MappingType[]> &tmp) {
  int max_size = GetMaxSize();
  for (int i = 0; i < (max_size - max_size / 2); i++) {
    array_[i] = tmp[i];
  }
  SetSize(max_size - max_size / 2);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Copy(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *page_p) {
  for (int i = GetSize(); i < GetSize() + page_p->GetSize(); i++) {
    array_[i] = page_p->array_[i - GetSize()];
  }
  IncreaseSize(page_p->GetSize());
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int {
  int l = 0;
  int r = GetSize() - 1;
  while (l <= r) {
    int mid = (l + r) / 2;
    int res = comparator(key, KeyAt(mid));
    if (res == 0) {
      CheckLegal(mid, " leaf keyindex ");
      return mid;
    }
    if (res < 0) {
      r = mid - 1;
    } else {
      l = mid + 1;
    }
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::PairAt(int index) const -> const MappingType & {
  CheckLegal(index, " leaf pairat ");
  const MappingType &mp = array_[index];
  return mp;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &comparator) {
  int index = KeyIndex(key, comparator);
  if (index == -1) {
    return;
  }
  for (int i = index + 1; i < GetSize(); i++) {
    array_[i - 1] = array_[i];
  }
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(int index) {
  CheckLegal(index, " leaf remove index ");
  for (int i = index + 1; i < GetSize(); i++) {
    array_[i - 1] = array_[i];
  }
  IncreaseSize(-1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
