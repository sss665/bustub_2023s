//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(1);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  KeyType key = array_[index].first;
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }
/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  ValueType val = array_[index].second;
  return val;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int {
  int l = 1;
  int r = GetSize()-1;
  while(l<=r){
	int mid = (l+r)/2;
	int res = comparator(key, KeyAt(mid));
	if(res<0){
	  r = mid-1;
	}else{
	  l = mid+1;
	}
  }
  return r;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindValue(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
  ValueType page_id;
  int index = KeyIndex(key, comparator);
  page_id = ValueAt(index);
  return page_id;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> bool {
  int l = 1;
  int r = GetSize()-1;
  while(l<=r){
	int mid = (l+r)/2;
	int res = comparator(key, KeyAt(mid));
	if(res==0){
	  return false;
	}
	if(res<0){
	  r = mid-1;
	}else{
	  l = mid+1;
	}
  }
  for (int j = GetSize() - 1; j >= l; j--) {
    array_[j + 1] = array_[j];
  }
  array_[l] = {key, value};
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(int index, MappingType mp) {
  for (int i = GetSize() - 1; i >= index; i++) {
    array_[i + 1] = array_[i];
  }
  array_[index] = mp;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyOut(std::shared_ptr<MappingType[]> &tmp) {
  int max_size = GetMaxSize();
  for (int i = 0; i < max_size; i++) {
    tmp[i] = array_[i];
  }
  // SetSize(index);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyInPre(std::shared_ptr<MappingType[]> &tmp) {
  int max_size = GetMaxSize();
  for (int i = 0; i < ((max_size - 1) / 2 + 1); i++) {
    array_[i] = tmp[i];
  }
  SetSize((max_size - 1) / 2 + 1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyInAfter(std::shared_ptr<MappingType[]> &tmp) {
  int max_size = GetMaxSize();
  int index = (max_size - 1) / 2 + 1;
  for (int i = index; i < max_size + 1; i++) {
    array_[i - index] = tmp[i];
  }
  SetSize(max_size - (max_size - 1) / 2);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Copy(BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> *page_p,
                                          KeyType &parent_key) {
  int size = GetSize();
  for (int i = GetSize(); i < GetSize() + page_p->GetSize(); i++) {
    array_[i] = page_p->array_[i - GetSize()];
  }
  SetKeyAt(size, parent_key);
  IncreaseSize(page_p->GetSize());
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  for (int i = index + 1; i < GetSize(); i++) {
    array_[i - 1] = array_[i];
  }
  IncreaseSize(-1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
