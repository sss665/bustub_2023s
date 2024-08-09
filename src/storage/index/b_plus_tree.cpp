#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto head_page = guard.As<BPlusTreeHeaderPage>();
  return head_page->root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // Declaration of context instance.
  if (IsEmpty()) {
    return false;
  }
  Context ctx;
  ReadPageGuard head_guard = bpm_->FetchPageRead(header_page_id_);
  auto head = head_guard.As<BPlusTreeHeaderPage>();
  page_id_t root_page_id = head->root_page_id_;
  auto guard = bpm_->FetchPageRead(root_page_id);
  auto cur = guard.As<BPlusTreePage>();
  while (!cur->IsLeafPage()) {
    ctx.read_set_.push_back(std::move(guard));
    auto internal = reinterpret_cast<const InternalPage *>(cur);
    page_id_t page_id = internal->FindValue(key, comparator_);
    guard = bpm_->FetchPageRead(page_id);
    cur = guard.As<BPlusTreePage>();
  }
  auto leaf = reinterpret_cast<const LeafPage *>(cur);
  bool res = leaf->FindValue(key, comparator_, result);
  head_guard.Drop();
  while (!ctx.read_set_.empty()) {
    ctx.read_set_.front().Drop();
    ctx.read_set_.pop_front();
  }
  guard.Drop();
  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  WritePageGuard head_guard = bpm_->FetchPageWrite(header_page_id_);
  auto head = head_guard.AsMut<BPlusTreeHeaderPage>();
  if (head->root_page_id_ == INVALID_PAGE_ID) {
    page_id_t page_id;
    BasicPageGuard guard = bpm_->NewPageGuarded(&page_id);
    auto root_page = guard.AsMut<LeafPage>();
    root_page->Init(leaf_max_size_);
    root_page->Insert(key, value, comparator_);
    head->root_page_id_ = page_id;
    return true;
  }
  Context ctx;
  page_id_t root_page_id = head->root_page_id_;
  ctx.header_page_ = std::move(head_guard);
  ctx.root_page_id_ = root_page_id;
  WritePageGuard write_guard = bpm_->FetchPageWrite(root_page_id);
  auto cur = write_guard.AsMut<BPlusTreePage>();
  page_id_t tmp_page_id = root_page_id;
  while (!cur->IsLeafPage()) {
    ctx.write_set_.push_back(std::move(write_guard));
    auto internal = reinterpret_cast<const InternalPage *>(cur);
    tmp_page_id = internal->FindValue(key, comparator_);
    write_guard = bpm_->FetchPageWrite(tmp_page_id);
    cur = write_guard.AsMut<BPlusTreePage>();
  }
  auto leaf = reinterpret_cast<LeafPage *>(cur);
  if (!leaf->Insert(key, value, comparator_)) {
    return false;
  }
  if (leaf->GetSize() == leaf->GetMaxSize()) {
    page_id_t page_id;
    BasicPageGuard new_guard = bpm_->NewPageGuarded(&page_id);
    auto new_page = new_guard.AsMut<LeafPage>();
    new_page->Init(leaf_max_size_);
    std::shared_ptr<MappingType[]> tmp(new MappingType[leaf->GetMaxSize()]);
    leaf->CopyOut(tmp);
    new_page->CopyIn(tmp);
    page_id_t next_page_id = leaf->GetNextPageId();
    /*if(next_page_id!=INVALID_PAGE_ID){
        WritePageGuard next_guard = bpm_->FetchPageWrite(next_page_id);
        auto right_tree_p = next_guard.AsMut<LeafPage>();
        right_tree_p->SetPrePageId(page_id);
    }*/
    new_page->SetNextPageId(next_page_id);
    // new_page->SetPrePageId(tmp_page_id);
    leaf->SetNextPageId(page_id);
    InsertParent(tmp[0].first, page_id, ctx, tmp_page_id);
  }
  ctx.header_page_->Drop();
  while (!ctx.write_set_.empty()) {
    ctx.write_set_.front().Drop();
    ctx.write_set_.pop_front();
  }
  write_guard.Drop();
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertParent(const KeyType &key, page_id_t page_id, Context &ctx, page_id_t page_id_1) {
  if (ctx.write_set_.empty()) {
    page_id_t new_page_id;
    BasicPageGuard new_guard = bpm_->NewPageGuarded(&new_page_id);
    auto new_page = new_guard.AsMut<InternalPage>();
    new_page->Init(internal_max_size_);
    new_page->Insert(key, page_id, comparator_);
    new_page->SetValueAt(0, page_id_1);
    WritePageGuard head_guard = std::move(*ctx.header_page_);
    auto head = head_guard.AsMut<BPlusTreeHeaderPage>();
    head->root_page_id_ = new_page_id;
    ctx.header_page_ = std::move(head_guard);
    return;
  }
  WritePageGuard parent_guard = std::move(ctx.write_set_.back());
  page_id_t old_page_id = parent_guard.PageId();
  auto cur = parent_guard.AsMut<InternalPage>();
  ctx.write_set_.pop_back();
  if (cur->GetSize() == cur->GetMaxSize()) {
    page_id_t new_page_id;
    BasicPageGuard new_guard = bpm_->NewPageGuarded(&new_page_id);
    auto new_page = new_guard.AsMut<InternalPage>();
    new_page->Init(internal_max_size_);
    std::shared_ptr<InternalType[]> tmp(new InternalType[cur->GetMaxSize() + 1]);
    cur->CopyOut(tmp);
    int i;
    for (i = 1; i < cur->GetMaxSize(); i++) {
      int res = comparator_(key, tmp[i].first);
      if (res < 0) {
        break;
      }
    }
    for (int j = cur->GetMaxSize() - 1; j >= i; j--) {
      tmp[j + 1] = tmp[j];
    }
    tmp[i] = {key, page_id};
    cur->CopyInPre(tmp);
    new_page->CopyInAfter(tmp);
    InsertParent(tmp[(cur->GetMaxSize() - 1) / 2 + 1].first, new_page_id, ctx, old_page_id);

  } else {
    cur->Insert(key, page_id, comparator_);
  }
  ctx.write_set_.push_back(std::move(parent_guard));
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  WritePageGuard head_guard = bpm_->FetchPageWrite(header_page_id_);
  auto head = head_guard.As<BPlusTreeHeaderPage>();
  if (head->root_page_id_ == INVALID_PAGE_ID) {
    return;
  }
  Context ctx;
  page_id_t root_page_id = head->root_page_id_;
  ctx.header_page_ = std::move(head_guard);
  ctx.root_page_id_ = root_page_id;
  WritePageGuard write_guard = bpm_->FetchPageWrite(root_page_id);
  auto cur = write_guard.AsMut<BPlusTreePage>();
  page_id_t tmp_page_id = root_page_id;
  while (!cur->IsLeafPage()) {
    ctx.write_set_.push_back(std::move(write_guard));
    auto internal = reinterpret_cast<const InternalPage *>(cur);
    tmp_page_id = internal->FindValue(key, comparator_);
    write_guard = bpm_->FetchPageWrite(tmp_page_id);
    cur = write_guard.AsMut<BPlusTreePage>();
  }
  Merge(ctx, key, write_guard);
  ctx.header_page_->Drop();
  while (!ctx.write_set_.empty()) {
    ctx.write_set_.front().Drop();
    ctx.write_set_.pop_front();
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Merge(Context &ctx, const KeyType &key, WritePageGuard &write_guard) {
  auto leaf = write_guard.AsMut<LeafPage>();
  leaf->Remove(key, comparator_);
  int max_size = leaf->GetMaxSize();
  int least_size = (max_size + 2) / 2 - 1;
  if (ctx.write_set_.empty()) {
    if (leaf->GetSize() == 0) {
      page_id_t write_guard_id = write_guard.PageId();
      write_guard.Drop();
      bpm_->DeletePage(write_guard_id);
      WritePageGuard head_guard = std::move(*ctx.header_page_);
      ctx.header_page_ = std::nullopt;
      auto head = head_guard.AsMut<BPlusTreeHeaderPage>();
      head->root_page_id_ = INVALID_PAGE_ID;
      ctx.header_page_ = std::move(head_guard);
    }
    return;
  }
  if (leaf->GetSize() < least_size) {
    WritePageGuard parent_guard = std::move(ctx.write_set_.back());
    auto internal_p = parent_guard.AsMut<InternalPage>();
    ctx.write_set_.pop_back();
    int index = internal_p->KeyIndex(key, comparator_);
    int bro_index;
    bool is_right;
    if (index != internal_p->GetSize() - 1) {
      bro_index = index + 1;
      is_right = true;
    } else {
      bro_index = index - 1;
      is_right = false;
    }
    page_id_t page_id = internal_p->ValueAt(bro_index);
    WritePageGuard guard = bpm_->FetchPageWrite(page_id);
    auto page_p = guard.AsMut<LeafPage>();
    if (page_p->GetSize() > least_size) {
      if (is_right) {
        MappingType mp = {page_p->KeyAt(0), page_p->ValueAt(0)};
        page_p->Remove(0);
        leaf->Insert(leaf->GetSize(), mp);
        internal_p->SetKeyAt(index + 1, page_p->KeyAt(0));
      } else {
        int size = page_p->GetSize();
        MappingType mp = {page_p->KeyAt(size - 1), page_p->ValueAt(size - 1)};
        page_p->IncreaseSize(-1);
        leaf->Insert(0, mp);
        internal_p->SetKeyAt(index, mp.first);
      }
      ctx.write_set_.push_back(std::move(write_guard));
      ctx.write_set_.push_back(std::move(guard));
    } else {
      if (is_right) {
        leaf->Copy(page_p);
        leaf->SetNextPageId(page_p->GetNextPageId());
        guard.Drop();
        bpm_->DeletePage(page_id);
        DeleteParent(parent_guard, ctx, index + 1, key);
        ctx.write_set_.push_back(std::move(write_guard));

      } else {
        page_p->Copy(leaf);
        page_p->SetNextPageId(leaf->GetNextPageId());
        page_id_t write_guard_id = write_guard.PageId();
        write_guard.Drop();
        bpm_->DeletePage(write_guard_id);
        DeleteParent(parent_guard, ctx, index, key);
        ctx.write_set_.push_back(std::move(guard));
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteParent(WritePageGuard &write_guard, Context &ctx, int index, const KeyType &key) {
  auto internal_p = write_guard.AsMut<InternalPage>();
  internal_p->Remove(index);
  if (ctx.write_set_.empty()) {
    if (internal_p->GetSize() == 1) {
      WritePageGuard head_guard = std::move(*ctx.header_page_);
      ctx.header_page_ = std::nullopt;
      auto head = head_guard.AsMut<BPlusTreeHeaderPage>();
      head->root_page_id_ = internal_p->ValueAt(0);
      page_id_t write_guard_id = write_guard.PageId();
      write_guard.Drop();
      bpm_->DeletePage(write_guard_id);
      ctx.header_page_ = std::move(head_guard);
    } else {
      ctx.write_set_.push_back(std::move(write_guard));
    }
    return;
  }
  int max_size = internal_p->GetMaxSize();
  int least_size = (max_size + 1) / 2 - 1;
  if (internal_p->GetSize() - 1 < least_size) {
    WritePageGuard parent_guard = std::move(ctx.write_set_.back());
    auto parent_p = parent_guard.AsMut<InternalPage>();
    ctx.write_set_.pop_back();
    int parent_index = parent_p->KeyIndex(key, comparator_);
    int bro_index;
    bool is_right;
    if (parent_index != parent_p->GetSize() - 1) {
      bro_index = parent_index + 1;
      is_right = true;
    } else {
      bro_index = parent_index - 1;
      is_right = false;
    }
    page_id_t page_id = parent_p->ValueAt(bro_index);
    WritePageGuard guard = bpm_->FetchPageWrite(page_id);
    auto page_p = guard.AsMut<InternalPage>();
    if (page_p->GetSize() - 1 > least_size) {
      if (is_right) {
        InternalType mp = {page_p->KeyAt(1), page_p->ValueAt(0)};
        page_p->Remove(0);
        KeyType parent_key = parent_p->KeyAt(parent_index + 1);
        parent_p->SetKeyAt(parent_index + 1, mp.first);
        internal_p->Insert(internal_p->GetSize(), {parent_key, mp.second});
      } else {
        int size = page_p->GetSize() - 1;
        InternalType mp = {page_p->KeyAt(size), page_p->ValueAt(size)};
        page_p->Remove(size);
        KeyType parent_key = parent_p->KeyAt(parent_index);
        parent_p->SetKeyAt(parent_index, mp.first);
        internal_p->SetKeyAt(0, parent_key);
        internal_p->Insert(0, {parent_key, mp.second});
      }
      ctx.write_set_.push_back(std::move(write_guard));
      ctx.write_set_.push_back(std::move(guard));
    } else {
      if (is_right) {
        KeyType parent_key = parent_p->KeyAt(parent_index + 1);
        internal_p->Copy(page_p, parent_key);
        guard.Drop();
        bpm_->DeletePage(page_id);
        DeleteParent(parent_guard, ctx, parent_index + 1, key);
        ctx.write_set_.push_back(std::move(write_guard));
      } else {
        KeyType parent_key = parent_p->KeyAt(parent_index);
        page_p->Copy(internal_p, parent_key);
        page_id_t write_guard_id = write_guard.PageId();
        write_guard.Drop();
        bpm_->DeletePage(write_guard_id);
        DeleteParent(parent_guard, ctx, parent_index, key);
        ctx.write_set_.push_back(std::move(guard));
      }
    }
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  ReadPageGuard head_guard = bpm_->FetchPageRead(header_page_id_);
  auto head = head_guard.As<BPlusTreeHeaderPage>();
  if (head->root_page_id_ == INVALID_PAGE_ID) {
    INDEXITERATOR_TYPE it;
    return it;
  }
  Context ctx;
  page_id_t root_page_id = head->root_page_id_;
  auto guard = bpm_->FetchPageRead(root_page_id);
  auto cur = guard.As<BPlusTreePage>();
  while (!cur->IsLeafPage()) {
    ctx.read_set_.push_back(std::move(guard));
    auto internal = reinterpret_cast<const InternalPage *>(cur);
    page_id_t page_id = internal->ValueAt(0);
    guard = bpm_->FetchPageRead(page_id);
    cur = guard.As<BPlusTreePage>();
  }
  page_id_t page_id = guard.PageId();
  INDEXITERATOR_TYPE it(page_id, 0, bpm_);
  head_guard.Drop();
  while (!ctx.read_set_.empty()) {
    ctx.read_set_.front().Drop();
    ctx.read_set_.pop_front();
  }
  guard.Drop();
  return it;
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  ReadPageGuard head_guard = bpm_->FetchPageRead(header_page_id_);
  auto head = head_guard.As<BPlusTreeHeaderPage>();
  if (head->root_page_id_ == INVALID_PAGE_ID) {
    INDEXITERATOR_TYPE it;
    return it;
  }
  Context ctx;

  page_id_t root_page_id = head->root_page_id_;
  auto guard = bpm_->FetchPageRead(root_page_id);
  auto cur = guard.As<BPlusTreePage>();
  while (!cur->IsLeafPage()) {
    ctx.read_set_.push_back(std::move(guard));
    auto internal = reinterpret_cast<const InternalPage *>(cur);
    page_id_t page_id = internal->FindValue(key, comparator_);
    guard = bpm_->FetchPageRead(page_id);
    cur = guard.As<BPlusTreePage>();
  }
  page_id_t page_id = guard.PageId();
  auto leaf = reinterpret_cast<const LeafPage *>(cur);
  int num = leaf->KeyIndex(key, comparator_);
  INDEXITERATOR_TYPE it(page_id, num, bpm_);
  head_guard.Drop();
  while (!ctx.read_set_.empty()) {
    ctx.read_set_.front().Drop();
    ctx.read_set_.pop_front();
  }
  guard.Drop();
  return it;
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  INDEXITERATOR_TYPE it;
  return it;
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
