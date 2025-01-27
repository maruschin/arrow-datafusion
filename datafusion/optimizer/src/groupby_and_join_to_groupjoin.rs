// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! [`GroupByAndJoinToGroupJoin`] Rewrites Group-By and Join to Group join

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::Transformed;
use datafusion_common::Result;
use datafusion_expr::expr_rewriter::coerce_plan_expr_for_schema;
use datafusion_expr::logical_plan::{JoinConstraint, JoinType, LogicalPlan};
use datafusion_expr::{Aggregate, Expr, GroupingSet, Join};
use itertools::Itertools;
use std::sync::Arc;

// Article: https://www.vldb.org/pvldb/vol4/p843-moerkotte.pdf
// Accelerating Queries with Group-By and Join by Groupjoin
#[derive(Default, Debug)]
pub struct GroupByAndJoinToGroupJoin {}

impl GroupByAndJoinToGroupJoin {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for GroupByAndJoinToGroupJoin {
    fn name(&self) -> &str {
        "group_by_and_join_to_group_join"
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Aggregate(Aggregate {
            input,
            group_expr,
            aggr_expr,
            schema: aggregate_schema,
            ..
        }) = plan.clone()
        else {
            return Ok(Transformed::no(plan));
        };

        let LogicalPlan::Join(Join {
            left,
            right,
            on,
            filter,
            join_type: JoinType::Left,
            join_constraint,
            schema: join_schema,
            null_equals_null,
            group_expr: _,
            aggr_expr: _,
        }) = Arc::unwrap_or_clone(input)
        else {
            return Ok(Transformed::no(plan));
        };

        if is_group_join(group_expr.clone(), on.clone()) {
            let new_plan = LogicalPlan::Join(Join {
                left,
                right,
                on,
                filter,
                join_type: JoinType::LeftGroup,
                join_constraint,
                schema: aggregate_schema,
                null_equals_null,
                group_expr: Some(group_expr),
                aggr_expr: Some(aggr_expr),
            });
            Ok(Transformed::yes(new_plan))
        } else {
            Ok(Transformed::no(plan))
        }
    }
}

fn is_group_join(group_expr: Vec<Expr>, on: Vec<(Expr, Expr)>) -> bool {
    dbg!(group_expr[0] == on[0].0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyzer::type_coercion::TypeCoercion;
    use crate::analyzer::Analyzer;
    use crate::test::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{config::ConfigOptions, Column};
    use datafusion_expr::{col, logical_plan::table_scan, LogicalPlanBuilder};
    use datafusion_functions_aggregate::expr_fn::{count, max, min};

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ])
    }

    fn assert_optimized_plan_equal(plan: LogicalPlan, expected: &str) -> Result<()> {
        assert_optimized_plan_eq(Arc::new(GroupByAndJoinToGroupJoin {}), plan, expected)
    }

    #[test]
    fn left_aggregated_and_join_right() -> Result<()> {
        let plan = table_scan(
            Some("left"),
            &Schema::new(vec![Field::new("key", DataType::UInt32, false)]),
            None,
        )?
        .join(
            table_scan(
                Some("right"),
                &Schema::new(vec![
                    Field::new("key", DataType::UInt32, false),
                    Field::new("value", DataType::UInt32, false),
                ]),
                None,
            )?
            .build()?,
            JoinType::Left,
            (vec!["left.key"], vec!["right.key"]),
            None,
        )?
        .aggregate(vec![col("left.key")], vec![max(col("value"))])?
        .build()?;

        let expected = "\
            Aggregate: groupBy=[[left.key]], aggr=[[max(right.value)]]\
            \n  Left Join: left.key = right.key\
            \n    TableScan: left\
            \n    TableScan: right\
            ";
        assert_optimized_plan_equal(plan, expected)
    }
}
