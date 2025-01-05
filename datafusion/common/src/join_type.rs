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

//! Defines the [`JoinType`], [`JoinConstraint`] and [`JoinSide`] types.

use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
};

use crate::error::_not_impl_err;
use crate::{DataFusionError, Result};

/// Join type
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Hash)]
pub enum JoinType {
    /// Inner Join - Returns only rows where there is a matching value in both tables based on the join condition.
    /// For example, if joining table A and B on A.id = B.id, only rows where A.id equals B.id will be included.
    /// All columns from both tables are returned for the matching rows. Non-matching rows are excluded entirely.
    Inner,
    /// Left Join - Returns all rows from the left table and matching rows from the right table.
    /// If no match, NULL values are returned for columns from the right table.
    /// Outer Right Join - Returns all rows from the right table and matching rows from the left table.
    /// If no match, NULL values are returned for columns from the left table.
    Outer(JoinSide),
    /// Outer Full Join (also called Full Outer Join) - Returns all rows from both tables, matching rows where possible.
    /// When a row from either table has no match in the other table, the missing columns are filled with NULL values.
    /// For example, if table A has row X with no match in table B, the result will contain row X with NULL values for all of table B's columns.
    /// This join type preserves all records from both tables, making it useful when you need to see all data regardless of matches.
    Full,
    /// Left Semi Join - Returns rows from the left table that have matching rows in the right table.
    /// Only columns from the left table are returned.
    /// Right Semi Join - Returns rows from the right table that have matching rows in the left table.
    /// Only columns from the right table are returned.
    Semi(JoinSide),
    /// Left Anti Join - Returns rows from the left table that do not have a matching row in the right table.
    /// Right Anti Join - Returns rows from the right table that do not have a matching row in the left table.
    Anti(JoinSide),
    /// Left Mark join
    ///
    /// Returns one record for each record from the left input. The output contains an additional
    /// column "mark" which is true if there is at least one match in the right input where the
    /// join condition evaluates to true. Otherwise, the mark column is false. For more details see
    /// [1]. This join type is used to decorrelate EXISTS subqueries used inside disjunctive
    /// predicates.
    ///
    /// Note: This we currently do not implement the full null semantics for the mark join described
    /// in [1] which will be needed if we and ANY subqueries. In our version the mark column will
    /// only be true for had a match and false when no match was found, never null.
    ///
    /// [1]: http://btw2017.informatik.uni-stuttgart.de/slidesandpapers/F1-10-37/paper_web.pdf
    LeftMark,
}

impl JoinType {
    pub fn is_outer(self) -> bool {
        match self {
            JoinType::Outer(_) | JoinType::Full => true,
            _ => false,
        }
    }

    /// Returns the `JoinType` if the (2) inputs were swapped
    ///
    /// Panics if [`Self::supports_swap`] returns false
    pub fn swap(&self) -> JoinType {
        match self {
            JoinType::Inner => JoinType::Inner,
            JoinType::Full => JoinType::Full,
            JoinType::Outer(side) => JoinType::Outer(side.negate()),
            JoinType::Semi(side) => JoinType::Semi(side.negate()),
            JoinType::Anti(side) => JoinType::Anti(side.negate()),
            JoinType::LeftMark => {
                unreachable!("LeftMark join type does not support swapping")
            }
        }
    }

    /// Does the join type support swapping  inputs?
    pub fn supports_swap(&self) -> bool {
        matches!(
            self,
            JoinType::Inner
                | JoinType::Full
                | JoinType::Outer(_)
                | JoinType::Semi(_)
                | JoinType::Anti(_)
        )
    }
}

impl Display for JoinType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let join_type = match self {
            JoinType::Inner => "Inner",
            JoinType::Outer(JoinSide::Left) => "Left",
            JoinType::Outer(JoinSide::Right) => "Right",
            JoinType::Full => "Full",
            JoinType::Semi(JoinSide::Left) => "LeftSemi",
            JoinType::Semi(JoinSide::Right) => "RightSemi",
            JoinType::Anti(JoinSide::Left) => "LeftAnti",
            JoinType::Anti(JoinSide::Right) => "RightAnti",
            JoinType::LeftMark => "LeftMark",
        };
        write!(f, "{join_type}")
    }
}

impl FromStr for JoinType {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self> {
        let s = s.to_uppercase();
        match s.as_str() {
            "INNER" => Ok(JoinType::Inner),
            "LEFT" => Ok(JoinType::Outer(JoinSide::Left)),
            "RIGHT" => Ok(JoinType::Outer(JoinSide::Right)),
            "FULL" => Ok(JoinType::Full),
            "LEFTSEMI" => Ok(JoinType::Semi(JoinSide::Left)),
            "RIGHTSEMI" => Ok(JoinType::Semi(JoinSide::Right)),
            "LEFTANTI" => Ok(JoinType::Anti(JoinSide::Left)),
            "RIGHTANTI" => Ok(JoinType::Anti(JoinSide::Right)),
            "LEFTMARK" => Ok(JoinType::LeftMark),
            _ => _not_impl_err!("The join type {s} does not exist or is not implemented"),
        }
    }
}

/// Join constraint
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Hash)]
pub enum JoinConstraint {
    /// Join ON
    On,
    /// Join USING
    Using,
}

impl Display for JoinSide {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            JoinSide::Left => write!(f, "left"),
            JoinSide::Right => write!(f, "right"),
        }
    }
}

/// Join side.
/// Stores the referred table side during calculations
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Hash)]
pub enum JoinSide {
    /// Left side of the join
    Left,
    /// Right side of the join
    Right,
}

impl JoinSide {
    /// Inverse the join side
    pub fn negate(&self) -> Self {
        match self {
            JoinSide::Left => JoinSide::Right,
            JoinSide::Right => JoinSide::Left,
        }
    }
}
