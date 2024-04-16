#pragma once

#include "sql/operator/physical_operator.h"

class Trx;
class SelectStmt;

/**
 * @brief 聚合算子
 * * @ingroup PhysicalOperator
 */
class AggregatePhysicalOperator : public PhysicalOperator
{
public:

  AggregatePhysicalOperator(){}

  virtual ~AggregatePhysicalOperator()=default;
  void add_aggregation(const AggrOp aggregation) {aggregations_.push_back(aggregation);}
  PhysicalOperatorType type() const override
  {
    return PhysicalOperatorType::AGGREGATE;
  }
    RC open(Trx *trx) override;
    RC next() override;
    RC close() override;

      Tuple *current_tuple() override {  return &result_tuple_; }

private:
  std::vector<AggrOp> aggregations_;
  ValueListTuple result_tuple_;
};
