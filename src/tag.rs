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
            if &p.table_name.trim_matches('"') == table_id {
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

/// Build a RecordBatch out of an artifact string
///
/// There are two variants that may be constructed. If the artifact
/// depth is equal to one, it will return a batch with just a sort
/// and tag column.
///
/// If the depth is higher than 1, any number of aggregate functions
/// may have been applied and need to be replayed. In this case an
/// additional tag column is added for each of those grouped tags.
///
/// Example:
/// > artifact_to_batches("[[[1,2],[3,4]]]")
/// . +-----------+----------+-----------+---------------+
/// . | __lsort__ | __ltag__ | __ltag2__ | __ltag3__     |
/// . +-----------+----------+-----------+---------------+
/// . | 1         | 1        | [1,2]     | [[1,2],[3,4]] |
/// . | 2         | 2        | [1,2]     | [[1,2],[3,4]] |
/// . | 3         | 3        | [3,4]     | [[1,2],[3,4]] |
/// . | 4         | 4        | [3,4]     | [[1,2],[3,4]] |
/// . +-----------+----------+-----------+---------------+
///
pub fn artifact_to_batches(artifact: String) -> Result<(u32, Vec<RecordBatch>)> {
    let depth = {
        let mut i = 0;
        for c in artifact.as_str().chars() {
            if c != '[' {
                break;
            }
            i += 1;
        }
        i
    };

    let mut fields = vec![
        Field::new("__lsort__", DataType::UInt64, false),
        Field::new("__ltag__", DataType::Utf8, false)
    ];

    for i in 2..depth+1 {
        fields.push(Field::new(format!("__ltag{}__", i).as_str(), DataType::Utf8, false))
    }

    let schema = Arc::new(Schema::new(fields));

    let mut int_builder = array::PrimitiveBuilder::<UInt64Type>::new(1);
    let mut str_builders: Vec<array::StringBuilder> = Vec::new();
    let mut buffers: Vec<String> = Vec::new();
    let mut sizes: Vec<u64> = Vec::new();
    for _ in 0..depth {
        str_builders.push(array::StringBuilder::new(1));
        buffers.push("".to_string());
        sizes.push(0);
    }

    let mut n: usize = 0;
    let max = depth as usize;

    for c in artifact.as_str().chars() {
        // Increase current layer by one
        if c == '[' {
            n += 1;
            for i in 0..n-1 {
                buffers[i].push(c);
            }

        // A comma signals that the value is complete and it can be appended
        } else if c == ',' {
            // Commit the buffer of the current layer
            if n == max {
                sizes[n-1] += 1;
                str_builders[n-1].append_value(buffers[n-1].clone())?;
                int_builder.append_value(sizes[n-1])?;

            // Outer layers may need to play catch-up
            } else {
                let gap = sizes[max-1] - sizes[n-1];
                for _ in 0..gap {
                    sizes[n-1] += 1;
                    str_builders[n-1].append_value(buffers[n-1].clone())?;
                }
            }
            buffers[n-1].truncate(0);

            // Simply append to the buffers of outer layers
            for i in 0..n-1 {
                buffers[i].push(c);
            }

        // Same as comma, but also decreases the current layer by one
        } else if c == ']' {
            if n == max {
                sizes[n-1] += 1;
                str_builders[n-1].append_value(buffers[n-1].clone())?;
                int_builder.append_value(sizes[n-1])?;
            } else {
                let gap = sizes[max-1] - sizes[n-1];
                for _ in 0..gap {
                    sizes[n-1] += 1;
                    str_builders[n-1].append_value(buffers[n-1].clone())?;
                }
            }
            buffers[n-1].truncate(0);

            for i in 0..n-1 {
                buffers[i].push(c);
            }

            n -= 1;

        // Just data, append to buffer
        } else {
            for i in 0..n {
                buffers[i].push(c);
            }
        }
    }

    let mut arrays: Vec<Arc<dyn datafusion::arrow::array::Array>> = vec![Arc::new(int_builder.finish())];
    for i in 1..max+1 {
        arrays.push(Arc::new(str_builders[max-i].finish()));
    }
    let batches = vec![RecordBatch::try_new(schema.clone(), arrays)?];

    Ok((depth, batches))
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

