/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by WangYunlai on 2021/6/9.
//

#include "sql/operator/update_physical_operator.h"
#include "sql/stmt/update_stmt.h"
#include "common/log/log.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"
#include   <vector>

using namespace std;

UpdatePhysicalOperator::UpdatePhysicalOperator(Table *table, Field field,Value value)
    : table_(table), field_(field),value_(value)
{}

RC UpdatePhysicalOperator::open(Trx *trx)
{
  if(children_.empty()) {
        return RC::SUCCESS;
    }
  std::unique_ptr<PhysicalOperator> &child = children_[0];
  RC                                 rc    = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  trx_ = trx;


  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::next() {

    RC rc= RC::SUCCESS;
    if(children_.empty()){
        return RC::RECORD_EOF;
    }

    PhysicalOperator *child=children_[0].get();

    //std::vector<Record> insert_records;
    while(RC::SUCCESS==(rc=child->next()))
    {
        Tuple * tuple=child->current_tuple();
     if (nullptr == tuple) {
      LOG_WARN("failed to get current record: %s", strrc(rc));
      return rc;
    }

  RowTuple *row_tuple=static_cast<RowTuple *>(tuple);
  Record &record = row_tuple->record();

  const char* field_name=field_.field_name();
  const FieldMeta *field_meta=table_->table_meta().field(field_name);

  int offset_=field_meta->offset();
  int len_=field_meta->len();

  rc=trx_->update_record(table_, record, offset_, len_, value_);

   // delete_records.emplace_back(record);
    //RC rc=RC::SUCCESS;
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to delete record: %s", strrc(rc));
      return rc;
    }
    /*else{
        const std::vector<FieldMeta> * table_field_metas =table_->table_meta().field_metas();
        const char *target_field_name=field_.field_name();

        int meta_num =table_field_metas->size();
        int target_index=-1;
        for(int i=0;i<meta_num;++i)
        {
            FieldMeta fieldmeta=(*table_field_metas)[i];
            const char*field_name =fieldmeta.name();
            if(0==strcmp(field_name,target_field_name)){
                target_index=i;
                break;
            }
        }
        //重新构造record
        std::vector<Value> values;
        int cell_num=row_tuple->cell_num();
        for(int i=0;i<cell_num;i++)
        {
        Record record;
        RC     rc = table_->make_record((value_.size()), value_.data(), record);
        rc = trx->insert_record(table_, record);
        }
    }*/

    }
    return RC::RECORD_EOF;
 }

RC UpdatePhysicalOperator::close() {
  if(!children_.empty()) {
        children_[0]->close();
    }
     return RC::SUCCESS; 
    }
