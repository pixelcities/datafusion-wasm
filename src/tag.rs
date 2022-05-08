use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType};
use datafusion::logical_plan::{plan, Expr, DFField, DFSchema, DFSchemaRef};
use datafusion::logical_plan::plan::LogicalPlan;
use datafusion::prelude::*;

use crate::string_agg::*;


/// Inject the row tag into each stage of the logical plan
pub fn inject(table_id: &String, plan: &LogicalPlan) -> std::result::Result<LogicalPlan, LogicalPlan> {
    match plan {
        LogicalPlan::Projection(p) => {
            match inject(table_id, &p.input) {
                Ok(child) => {
                    // Check if the Expr already contains the row tag (e.g. SELECT *)
                    let expr = if p.expr.iter().any(|x| x.name(&p.schema).unwrap().contains("__tag__")) {
                        p.expr.clone()
                    } else {
                        [p.expr.clone(), vec![col("__tag__")]].concat()
                    };

                    // Same for the schema
                    let schema = _inject_schema(&p.schema);

                    return Ok(LogicalPlan::Projection(plan::Projection{
                        expr: expr,
                        input: Arc::new(child),
                        schema: schema,
                        alias: p.alias.clone()
                    }))
                },
                _ => Err(LogicalPlan::Projection(p.clone()))
            }
        },

        LogicalPlan::Aggregate(p) => {
            match inject(table_id, &p.input) {
                Ok(child) => {
                    let string_agg = string_agg_fn();
                    let agg = Expr::Alias(
                        Box::new(string_agg.call(vec![col("__tag__")])),
                        "__tag__".to_string()
                    );

                    return Ok(LogicalPlan::Aggregate(plan::Aggregate{
                        input: Arc::new(child),
                        group_expr: p.group_expr.clone(),
                        aggr_expr: [p.aggr_expr.clone(), vec![agg]].concat(),
                        schema: _inject_schema(&p.schema)
                    }))
                },
                _ => Err(LogicalPlan::Aggregate(p.clone()))
            }
        },

        LogicalPlan::Filter(p) => {
            match inject(table_id, &p.input) {
                Ok(child) => {
                    let mut result = p.clone();
                    result.input = Arc::new(child);
                    Ok(LogicalPlan::Filter(result))
                },
                Err(_) => Err(LogicalPlan::Filter(p.clone()))
            }
        },

        LogicalPlan::Window(p) => {
            match inject(table_id, &p.input) {
                Ok(child) => {
                    let mut result = p.clone();
                    result.input = Arc::new(child);
                    result.schema = _inject_schema(&p.schema);
                    Ok(LogicalPlan::Window(result))
                },
                Err(_) => Err(LogicalPlan::Window(p.clone()))
            }
        },

        LogicalPlan::Sort(p) => {
            match inject(table_id, &p.input) {
                Ok(child) => {
                    let mut result = p.clone();
                    result.input = Arc::new(child);
                    Ok(LogicalPlan::Sort(result))
                },
                Err(_) => Err(LogicalPlan::Sort(p.clone()))
            }
        },

        LogicalPlan::Limit(p) => {
            match inject(table_id, &p.input) {
                Ok(child) => {
                    let mut result = p.clone();
                    result.input = Arc::new(child);
                    Ok(LogicalPlan::Limit(result))
                },
                Err(_) => Err(LogicalPlan::Limit(p.clone()))
            }
        },

        LogicalPlan::Join(p) => {
            let left = inject(table_id, &p.left);
            let right = inject(table_id, &p.right);

            if left.as_ref().or(right.as_ref()).is_ok() {
                let (Ok(left) | Err(left)) = left;
                let (Ok(right) | Err(right)) = right;

                Ok(LogicalPlan::Join(plan::Join{
                    left: Arc::new(left),
                    right: Arc::new(right),
                    on: p.on.clone(),
                    join_type: p.join_type,
                    join_constraint: p.join_constraint,
                    schema: _inject_schema(&p.schema),
                    null_equals_null: p.null_equals_null
                }))
            } else {
                Err(LogicalPlan::Join(p.clone()))
            }
        },

        LogicalPlan::CrossJoin(p) => {
            let left = inject(table_id, &p.left);
            let right = inject(table_id, &p.right);

            if left.as_ref().or(right.as_ref()).is_ok() {
                let (Ok(left) | Err(left)) = left;
                let (Ok(right) | Err(right)) = right;

                Ok(LogicalPlan::CrossJoin(plan::CrossJoin{
                    left: Arc::new(left),
                    right: Arc::new(right),
                    schema: _inject_schema(&p.schema)
                }))
            } else {
                Err(LogicalPlan::CrossJoin(p.clone()))
            }
        },

        LogicalPlan::TableScan(p) => {
            if &p.table_name == table_id {
                Ok(LogicalPlan::TableScan(p.clone()))
            } else {
                Err(LogicalPlan::TableScan(p.clone()))
            }
        },

        p => Err(p.clone())
    }
}


fn _inject_schema(schema: &DFSchemaRef) -> DFSchemaRef {
    match schema.field_with_unqualified_name("__tag__") {
        Ok(_) => schema.clone(),
        Err(_) => {
            let field = DFField::new(None, "__tag__", DataType::Utf8, false);
            Arc::new(schema.clone().join(&DFSchema::new(vec![field]).unwrap()).unwrap())
        }
    }
}

