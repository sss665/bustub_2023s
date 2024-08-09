#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
		plan_(plan),
		child_executor_(std::move(child_executor)),
		func_([this](const Tuple& tuple_1, const Tuple& tuple_2) -> bool{
			for(auto pairs : plan_->GetOrderBy()){
				auto value1 = pairs.second->Evaluate(&tuple_1,child_executor_->GetOutputSchema());
				auto value2 = pairs.second->Evaluate(&tuple_2,child_executor_->GetOutputSchema());
				switch (pairs.first) {
					case OrderByType::INVALID:
						std::cerr << "invalid OrderByType"<< std::endl;
						break;
					case OrderByType::DEFAULT:
					case OrderByType::ASC:
						if(value1.CompareLessThan(value2)==CmpBool::CmpTrue){
							return true;
						}
						if(value1.CompareGreaterThan(value2)==CmpBool::CmpTrue){
							return false;
						}
						break;
					case OrderByType::DESC:
						if(value1.CompareLessThan(value2)==CmpBool::CmpTrue){
							return false;
						}
						if(value1.CompareGreaterThan(value2)==CmpBool::CmpTrue){
							return true;
						}
						break;
				}
			}
			return true;
		}),
		priority_queue_(func_){}

void TopNExecutor::Init() {
	child_executor_->Init();
	while (!priority_queue_.empty()) {
		priority_queue_.pop();
	}
	while (!res_.empty()) {
		res_.pop();
	}
	Tuple tuple;
	RID rid;
	while(true){
		const auto status = child_executor_->Next(&tuple,&rid);
		if(!status){
			break;
		}
		if(priority_queue_.size()<plan_->GetN()){
			priority_queue_.push(tuple);
		}else if(func_(tuple,priority_queue_.top())){
			priority_queue_.pop();
			priority_queue_.push(tuple);
		}
	}
	while(!priority_queue_.empty()){
		res_.push(priority_queue_.top());
		priority_queue_.pop();
	}
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
	if(res_.empty()){
		return false;
	}
	*tuple = res_.top();
	res_.pop();
	return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return 0;};

}  // namespace bustub
