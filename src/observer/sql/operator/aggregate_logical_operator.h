#pragma once
#include <memory>
#include <vector>

#include "sql/operator/logical_operator.h"
#include "sql/expr/expression.h"
#include "storage/field/field.h"

class AggregateLogicalOperator : public LogicalOperator
{
public:
  AggregateLogicalOperator(const std::vector<Field> &field): fields_(field) {}
  //AggregateLogicalOperator(std::vector<Field, std::allocator<Field> > const&);
  virtual ~AggregateLogicalOperator() = default;

  LogicalOperatorType type() const override { 
    return LogicalOperatorType::AGGREGATE; }

  const std::vector<Field> &fields() const {
    return fields_;
  }

private:
  std::vector<Field> fields_;
};