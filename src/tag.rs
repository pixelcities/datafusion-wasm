use std::sync::Arc;
use uuid::Uuid;

use datafusion::arrow::array;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Schema, Field, UInt64Type};
use datafusion::execution::dataframe_impl::DataFrameImpl;
use datafusion::logical_plan::{plan, Expr, DFField, DFSchema, DFSchemaRef};
use datafusion::logical_plan::plan::LogicalPlan;
use datafusion::physical_plan::window_functions::{WindowFunction, BuiltInWindowFunction};
use datafusion::datasource::MemTable;
use datafusion::error::Result;
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

pub async fn register_table_with_tag(ctx: &mut ExecutionContext, table_id: &String, schema: Arc<Schema>, batches: Vec<RecordBatch>) -> Result<()> {
    let provider = MemTable::try_new(schema.clone(), vec![batches])?;

    let id = Uuid::new_v4().to_hyphenated().to_string();
    ctx.register_table(id.as_str(), Arc::new(provider))?;

    // Recreate the table with an additional column containing the original row numbers
    let mut select: Vec<Expr> = schema.fields().into_iter().map(|field| col(&field.name().clone())).collect();

    select.push(Expr::Alias(
            Box::new(Expr::WindowFunction {
                fun: WindowFunction::BuiltInWindowFunction(BuiltInWindowFunction::RowNumber),
                args: vec![],
                partition_by: vec![],
                order_by: vec![],
                window_frame: None,
            }),
        "__tag__".to_string()
    ));

    // Rebuild the table with row tags
    let df = ctx.table(id.as_str())?.select(select)?;
    let batches = df.collect().await?;
    let provider = MemTable::try_new(batches[0].schema(), vec![batches])?;
    let id = Uuid::new_v4().to_hyphenated().to_string();
    ctx.register_table(id.as_str(), Arc::new(provider))?;

    // Stage 2 (cast), and register with the real table name
    let mut select: Vec<Expr> = schema.fields().into_iter().map(|field| col(&field.name().clone())).collect();
    select.push(Expr::Alias(
        Box::new(Expr::Cast{
            expr: Box::new(col("__tag__")),
            data_type: DataType::Utf8
        }),
        "__tag__".to_string()
    ));
    let df = ctx.table(id.as_str())?.select(select)?;
    let table = Arc::new(DataFrameImpl::new(ctx.state.clone(), &df.to_logical_plan()));
    ctx.register_table(table_id.as_str(), table)?;

    Ok(())
}

pub fn artifact_to_batches(artifact: String) -> Result<Vec<RecordBatch>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("__ltag__", DataType::Utf8, false),
        Field::new("__lsort__", DataType::UInt64, false)
    ]));

    let mut str_builder = array::StringBuilder::new(1);
    let mut int_builder = array::PrimitiveBuilder::<UInt64Type>::new(1);

    for (i, tag) in artifact.trim_start_matches('[').trim_end_matches(']').split(',').enumerate() {
        str_builder.append_value(tag)?;
        int_builder.append_value(i as u64)?;
    }
    let str_arr = Arc::new(str_builder.finish());
    let int_arr = Arc::new(int_builder.finish());
    let batches = vec![RecordBatch::try_new(schema.clone(), vec![str_arr, int_arr])?];

    Ok(batches)
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

