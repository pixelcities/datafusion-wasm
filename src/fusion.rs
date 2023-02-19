extern crate console_error_panic_hook;

use wasm_bindgen::prelude::*;
use web_sys::console;
use js_sys::{Promise, Object, Uint8Array};
use serde_json::value::Value;
use serde_json::json;

use std::io::Cursor;
use std::sync::Arc;
use std::cell::RefCell;
use std::collections::hash_map::HashMap;
use std::str::FromStr;

use uuid::Uuid;

use datafusion::arrow::array;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, TimeUnit, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::ipc::reader::FileReader;
use datafusion::arrow::ipc::writer::FileWriter;
use datafusion::arrow::csv::writer::Writer;

use datafusion::logical_plan::{plan, Expr};
use datafusion::physical_plan::aggregates;
use datafusion::execution::dataframe_impl::DataFrameImpl;
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::prelude::*;

use crate::tag::*;
use crate::synth::*;
use crate::utils::*;


struct Table {
    batches: Vec<RecordBatch>
}

pub struct DataFusionInner {
    tables: RefCell<HashMap<String, Table>>
}

#[wasm_bindgen]
pub struct DataFusion {
    inner: Arc<DataFusionInner>
}

#[wasm_bindgen]
impl DataFusion {
    #[wasm_bindgen(constructor)]
    pub fn new() -> DataFusion {
        console_error_panic_hook::set_once();

        DataFusion { inner: Arc::new(DataFusionInner { tables: RefCell::new(HashMap::new()) })}
    }

    pub fn table_exists(&self, table_id: String) -> bool {
        self.inner.tables.borrow().contains_key(&table_id)
    }

    pub fn nr_tables(&self) -> usize {
        self.inner.tables.borrow().len()
    }

    pub fn nr_rows(&self, table_id: String) -> usize {
        match self.inner.tables.borrow().get(&table_id) {
            Some(table) => {
                let mut rows: usize = 0;

                for batch in &table.batches {
                    rows += batch.num_rows();
                }

                rows
            },
            None => 0
        }
    }

    pub fn get_schema(&self, table_id: String) -> JsValue {
        match self.inner.tables.borrow().get(&table_id) {
            Some(table) => {
                if table.batches.len() > 0 {
                    let batch = &table.batches[0];
                    let schema = batch.schema();

                    JsValue::from_serde(&schema.to_json()).unwrap()

                } else {
                    console::log_1(&"Unable to get schema: empty table".into());
                    JsValue::undefined()
                }
            },
            None => {
                console::log_1(&"Invalid table id".into());
                JsValue::undefined()
            }
        }
    }

    pub fn get_row(&self, table_id: String, row_id: usize) -> Object {
        let obj = js_sys::Object::new();

        match self.inner.tables.borrow().get(&table_id) {
            Some(table) => {
                let mut rows: usize = 0;
                let mut batch_id = 0;

                for batch in &table.batches {
                    if rows + batch.num_rows() > row_id {
                        break;
                    }

                    rows += batch.num_rows();
                    batch_id += 1;
                }

                if &table.batches.len() > &batch_id {
                    let batch = &table.batches[batch_id];
                    let schema = batch.schema();

                    let mut i: usize = 0;
                    for column in batch.columns() {
                        let row = column.slice(row_id - rows, 1);

                        let key: JsValue = schema.field(i).name().into();
                        let value: JsValue = if !row.is_null(0) {
                            match schema.field(i).data_type() {
                                DataType::Int32 => row.as_any().downcast_ref::<array::Int32Array>().expect("").value(0).into(),
                                DataType::Float64 => row.as_any().downcast_ref::<array::Float64Array>().expect("").value(0).into(),
                                DataType::Utf8 => row.as_any().downcast_ref::<array::StringArray>().expect("").value(0).into(),
                                DataType::Boolean => row.as_any().downcast_ref::<array::BooleanArray>().expect("").value(0).to_string().into(),
                                DataType::Timestamp(TimeUnit::Second, None) => row.as_any().downcast_ref::<array::TimestampSecondArray>().expect("").value(0).into(),
                                DataType::Null => JsValue::null(),

                                // Gets cast to BigInt, which is not that common. Just force Number instead
                                DataType::Int64 => (row.as_any().downcast_ref::<array::Int64Array>().expect("").value(0) as f64).into(),

                                // Supporting all lists is tedious, and as the data is returned for display
                                // only, we just cast it to a string representation of the array.
                                DataType::List(_) => {
                                    let list = row.as_any().downcast_ref::<array::ListArray>().expect("");
                                    to_string_array(list.value_type(), list.value(0)).into()
                                },

                                // Not implemented
                                other => {
                                    console::log_2(&"Warning: unexpected data type: ".into(), &json!(other).to_string().into());
                                    JsValue::null()
                                }
                            }
                        } else {
                            JsValue::null()
                        };

                        js_sys::Reflect::set(&obj, &key, &value).unwrap();

                        i += 1;
                    }
                }
            },
            None => {}
        }

        obj
    }

    pub fn describe_table(&self, table_id: String) -> Promise {
        let _self = self.inner.clone();
        let batches = _self.tables.borrow().get(&table_id).unwrap_or(&Table { batches: vec![] }).batches.clone();

        if batches.len() > 0 {
            let schema = batches[0].schema();

            wasm_bindgen_futures::future_to_promise(async move {
                let description = describe(&table_id, schema, batches, None).await;
                match description {
                    Ok(result) => {
                        Ok(JsValue::from_serde(&json!(result)).unwrap())
                    },
                    Err(e) => {
                        console::log_2(&"Error describing column:".into(), &e.to_string().into());

                        Err(JsValue::undefined())
                    },
                }
            })
        } else {
            console::log_1(&"Unable to describe table: empty table".into());
            wasm_bindgen_futures::future_to_promise(std::future::ready(Err(JsValue::undefined())))
        }
    }

    pub fn synthesize_table(&self, in_table_id: String, out_table_id: String, epsilon: f64) -> Promise {
        let _self = self.inner.clone();
        let batches = _self.tables.borrow().get(&in_table_id).unwrap_or(&Table { batches: vec![]}).batches.clone();

        if batches.len() > 0 {
            let schema = batches[0].schema();

            wasm_bindgen_futures::future_to_promise(async move {
                match describe(&in_table_id, schema.clone(), batches, None).await {
                    Ok(result) => {
                        let description = add_laplace_noise(result, epsilon);
                        let arrays = gen_synthethic_dataset(description);

                        match RecordBatch::try_new(schema, arrays) {
                            Ok(batch) => {
                                match _self.tables.try_borrow_mut() {
                                    Ok(mut tables) => {
                                        tables.insert(out_table_id.clone(), Table { batches: vec![batch] });
                                        Ok(out_table_id.into())
                                    },
                                    Err(_) => Err(JsValue::undefined())
                                }
                            },
                            Err(e) => {
                                console::log_2(&"Error building synthesized table:".into(), &e.to_string().into());
                                Err(JsValue::undefined())
                            }
                        }
                    },
                    Err(e) => {
                        console::log_2(&"Error describing column:".into(), &e.to_string().into());
                        Err(JsValue::undefined())
                    },
                }
            })
        } else {
            console::log_1(&"Unable to synthesize table: empty table".into());
            wasm_bindgen_futures::future_to_promise(std::future::ready(Err(JsValue::undefined())))
        }
    }

    pub fn update_schema(&self, table_id: String, schema: JsValue) -> () {
        let table_exists = self.inner.tables.borrow().contains_key(&table_id);

        if table_exists {
            let batches = self.inner.tables.borrow().get(&table_id).unwrap().batches.clone();
            let json: Value = JsValue::into_serde(&schema).unwrap();
            let schema = Schema::from(&json).unwrap();

            if batches.len() > 0 {
                let batches: Vec<RecordBatch> = batches.clone().into_iter()
                    .map(|b| {
                        // Batches may have metadata for list types, which should be discarded
                        let columns: Vec<ArrayRef> = b.columns().into_iter().map(|c| discard_nested_metadata(c)).collect();

                        RecordBatch::try_new(Arc::new(schema.clone()), columns.to_vec()).unwrap()
                    })
                    .collect();

                match self.inner.tables.try_borrow_mut() {
                    Ok(mut tables) => {
                        tables.insert(table_id, Table { batches: batches });
                    },
                    Err(_) => {
                        console::log_1(&"Cannot update schema: a table is immutably borrowed".into());
                    }
                }
            }
        } else {
            console::log_1(&"Cannot update schema: invalid table id".into());
        };
    }

    pub fn drop_columns(&self, table_id: String, drop_columns: JsValue) -> () {
        let batches = self.inner.tables.borrow().get(&table_id).unwrap_or(&Table { batches: vec![] }).batches.clone();
        let num_batches = batches.len();
        let columns: Vec<String> = JsValue::into_serde(&drop_columns).unwrap();

        if num_batches > 0 {
            let schema = batches[0].schema();

            let batches = (0..num_batches).map(|batch_id| {
                let n = batches[batch_id].num_columns();
                let arrays = (0..n).filter_map(|i| {
                    let field_name = schema.field(i).name();

                    if !columns.contains(&field_name) {
                        Some(batches[batch_id].column(i).clone())
                    } else {
                        None
                    }
                }).collect();

                let fields = (0..n).filter_map(|i| {
                    let field_name = schema.field(i).name();

                    if !columns.contains(&field_name) {
                        Some(schema.field(i).clone())
                    } else {
                        None
                    }
                }).collect();

                RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
            }).collect();

            match self.inner.tables.try_borrow_mut() {
                Ok(mut tables) => {
                    tables.insert(table_id, Table { batches: batches });
                },
                Err(_) => {
                    console::log_1(&"Cannot drop columns: a table is immutably borrowed".into());
                }
            }
        } else {
            console::log_1(&"Cannot drop columns: invalid table id".into());
        };
    }

    pub fn load_table(&self, table: &[u8], table_id: String) -> String {
        let id = if table_id.is_empty() {
            Uuid::new_v4().to_hyphenated().to_string()
        } else {
            table_id
        };

        let buf = Cursor::new(table);
        let reader = FileReader::try_new(buf).unwrap();
        let batches: Vec<RecordBatch> = reader
            .map(|b| b.unwrap())
            .collect();

        if batches.len() > 0 {
            match self.inner.tables.try_borrow_mut() {
                Ok(mut tables) => {
                    tables.insert(id.clone(), Table { batches: batches });
                },
                Err(_) => {
                    console::log_1(&"Cannot load table: a table is immutably borrowed".into());
                }
            }
        }

        id
    }

    pub fn clone_table(&self, table_id: String, clone_id: String) -> String {
        let id = if clone_id.is_empty() {
            Uuid::new_v4().to_hyphenated().to_string()
        } else {
            if self.inner.tables.borrow().contains_key(&clone_id) {
                match self.inner.tables.try_borrow_mut() {
                    Ok(mut tables) => {
                        tables.remove(&clone_id);
                    },
                    Err(_) => {}
                }
            }

            clone_id
        };

        let batches  = self.inner.tables.borrow().get(&table_id).unwrap().batches.clone();
        match self.inner.tables.try_borrow_mut() {
            Ok(mut tables) => {
                tables.insert(id.clone(), Table { batches: batches });
            },
            Err(_) => {
                console::log_1(&"Cannot clone table: a table is immutably borrowed".into());
            }
        };

        id
    }

    pub fn append_table(&self, table_id: String, append_id: String) -> () {
        let in_batches = self.inner.tables.borrow().get(&table_id).unwrap_or(&Table { batches: vec![] }).batches.clone();
        let append_batches = self.inner.tables.borrow().get(&append_id).unwrap_or(&Table { batches: vec![] }).batches.clone();

        if in_batches.len() > 0 && append_batches.len() > 0 {
            let in_schema = in_batches[0].schema();
            let append_schema = append_batches[0].schema();

            let schema = Schema::try_merge(vec![(*in_schema).clone(), (*append_schema).clone()]).unwrap();

            let num_batches = in_batches.len();
            let batches = (0..num_batches).map(|batch_id| {
                let in_arrays = in_batches[batch_id].columns();
                let append_arrays = append_batches[batch_id].columns();

                RecordBatch::try_new(Arc::new(schema.clone()), [in_arrays, append_arrays].concat()).unwrap()
            }).collect();

            match self.inner.tables.try_borrow_mut() {
                Ok(mut tables) => {
                    tables.insert(table_id, Table { batches: batches });
                    tables.remove(&append_id);
                },
                Err(_) => {
                    console::log_1(&"Cannot append table: a table is immutably borrowed".into());
                }
            };
        }
    }

    pub fn merge_table(&self, table_id: String, merge_id: String) -> () {
        let in_batches = self.inner.tables.borrow().get(&table_id).unwrap_or(&Table { batches: vec![] }).batches.clone();
        let merge_batches = self.inner.tables.borrow().get(&merge_id).unwrap_or(&Table { batches: vec![] }).batches.clone();

        if in_batches.len() > 0 && merge_batches.len() > 0 {
            let in_schema = in_batches[0].schema();
            let merge_schema = merge_batches[0].schema();

            let num_batches = in_batches.len();
            let batches = (0..num_batches).map(|batch_id| {
                let n = in_batches[batch_id].num_columns();
                let arrays = (0..n).map(|i| {
                    let field_name = in_schema.field(i).name();

                    match merge_schema.index_of(field_name) {
                        Ok(j) => merge_batches[batch_id].column(j).clone(),
                        Err(_) => in_batches[batch_id].column(i).clone()
                    }
                }).collect();

                let fields = (0..n).map(|i| {
                    let field_name = in_schema.field(i).name();

                    match merge_schema.index_of(field_name) {
                        Ok(j) => merge_schema.field(j).clone(),
                        Err(_) => in_schema.field(i).clone()
                    }
                }).collect();

                RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
            }).collect();

            match self.inner.tables.try_borrow_mut() {
                Ok(mut tables) => {
                    tables.insert(table_id, Table { batches: batches });
                    tables.remove(&merge_id);
                },
                Err(_) => {
                    console::log_1(&"Cannot merge table: a table is immutably borrowed".into());
                }
            };
        }
    }

    pub fn move_table(&self, table_id: String, target_id: String) -> () {
        let batches  = self.inner.tables.borrow().get(&table_id).unwrap().batches.clone();

        match self.inner.tables.try_borrow_mut() {
            Ok(mut tables) => {
                tables.remove(&table_id);
                tables.remove(&target_id);

                tables.insert(target_id, Table { batches: batches });
            },
            Err(_) => {
                console::log_1(&"Cannot move table: a table is immutably borrowed".into());
            }
        };
    }

    pub fn drop_table(&self, table_id: String) {
        match self.inner.tables.try_borrow_mut() {
            Ok(mut tables) => {
                tables.remove(&table_id);
            },
            Err(_) => {
                console::log_1(&"Cannot drop table: a table is immutably borrowed".into());
            }
        };
    }

    pub fn query(&self, table_id: String, query: String) -> Promise {
        let _self = self.inner.clone();
        let batches = _self.tables.borrow().get(&table_id).unwrap_or(&Table { batches: vec![] }).batches.clone();

        if batches.len() > 0 {
            let schema = batches[0].schema();

            wasm_bindgen_futures::future_to_promise(async move {
                let batches = execute(&table_id, schema, batches, query).await;
                match batches {
                    Ok((batches, artifact)) => {
                        match _self.tables.try_borrow_mut() {
                            Ok(mut tables) => {
                                tables.insert(table_id, Table { batches: batches });
                                Ok(artifact.into())
                            },
                            Err(_) => {
                                console::log_1(&"Cannot query table: a table is immutably borrowed".into());
                                Err(JsValue::undefined())
                            }
                        }
                    },
                    Err(e) => {
                        console::log_2(&"Error executing query:".into(), &e.to_string().into());

                        Err(JsValue::undefined())
                    },
                }
            })
        } else {
            console::log_1(&"Cannot query: empty table".into());
            wasm_bindgen_futures::future_to_promise(std::future::ready(Err(JsValue::undefined())))
        }
    }

    pub fn join(&self, table_id: String, left_id: String, right_id: String, query: String) -> Promise {
        let _self = self.inner.clone();

        let left_batches = _self.tables.borrow().get(&left_id).unwrap_or(&Table { batches: vec![] }).batches.clone();
        let right_batches = _self.tables.borrow().get(&right_id).unwrap_or(&Table { batches: vec![] }).batches.clone();

        if left_batches.len() > 0 && right_batches.len() > 0 {
            let left_schema = left_batches[0].schema();
            let right_schema = right_batches[0].schema();

            wasm_bindgen_futures::future_to_promise(async move {
                match join(&left_id, &right_id, left_schema, right_schema, left_batches, right_batches, query).await {
                    Ok((batches, (left_artifact, right_artifact))) => {
                        match _self.tables.try_borrow_mut() {
                            Ok(mut tables) => {
                                tables.insert(table_id, Table { batches: batches });

                                Ok(JsValue::from_serde(&json!([left_artifact, right_artifact])).unwrap())
                            },
                            Err(_) => {
                                console::log_1(&"Cannot join: a table is immutably borrowed".into());
                                Err(JsValue::undefined())
                            }
                        }
                    },
                    Err(e) => {
                        console::log_2(&"Cannot join:".into(), &e.to_string().into());
                        Err(JsValue::undefined())
                    },
                }
            })
        } else {
            console::log_1(&"Cannot join: one or more of the tables are empty".into());
            wasm_bindgen_futures::future_to_promise(std::future::ready(Err(JsValue::undefined())))
        }
    }

    pub fn apply_artifact(&self, table_id: String, artifact: String) -> Promise {
        let _self = self.inner.clone();
        let batches = _self.tables.borrow().get(&table_id).unwrap_or(&Table { batches: vec![] }).batches.clone();

        if batches.len() > 0 {
            let schema = batches[0].schema();

            wasm_bindgen_futures::future_to_promise(async move {
                let batches = apply_artifact(&table_id, schema, batches, artifact).await;
                match batches {
                    Ok(batches) => {
                        match _self.tables.try_borrow_mut() {
                            Ok(mut tables) => {
                                tables.insert(table_id, Table { batches: batches });
                                Ok(JsValue::undefined())
                            },
                            Err(_) => {
                                console::log_1(&"Cannot apply artifact: a table is immutably borrowed".into());
                                Err(JsValue::undefined())
                            }
                        }
                    },
                    Err(_) => {
                        console::log_1(&"Error applying artifact".into());

                        Err(JsValue::undefined())
                    },
                }
            })
        } else {
            console::log_1(&"Cannot apply artifact: empty table".into());
            wasm_bindgen_futures::future_to_promise(std::future::ready(Err(JsValue::undefined())))
        }
    }

    pub fn export_table(&self, table_id: String) -> Uint8Array {
        let batches  = self.inner.tables.borrow().get(&table_id).unwrap_or(&Table { batches: vec![] }).batches.clone();

        if batches.len() > 0 {
            let schema = batches[0].schema();

            let mut buf = Vec::new();
            {
                let mut writer = FileWriter::try_new(&mut buf, &schema).unwrap();

                for batch in &batches {
                    writer.write(&batch).unwrap();
                }

                writer.finish().unwrap();
            }
            let result = &buf[..];

            result.into()
        } else {
            Uint8Array::new_with_length(0)
        }
    }

    pub fn export_csv(&self, table_id: String) -> String {
        let batches  = self.inner.tables.borrow().get(&table_id).unwrap_or(&Table { batches: vec![] }).batches.clone();

        let mut buf = Vec::new();
        {
            let mut writer = Writer::new(&mut buf);

            for batch in &batches {
                writer.write(&batch).unwrap();
            }
        }
        let result = &buf[..];

        String::from_utf8_lossy(result).into_owned()
    }
}

/// Execute a given query, returning the result and an artifact
///
/// The given table is first extended by adding row numbers, which are dropped
/// again before returning the resulting batches. Instead, the row numbers travel
/// alongside the given query so that the original row numbers get shuffled around
/// and are able to capture the mutations to the row order.
///
/// The query artifact can be used to apply the same order / unnest operations on
/// dataset fragments that are not part of the table in this context.
async fn execute(table_id: &String, schema: Arc<Schema>, batches: Vec<RecordBatch>, query: String) -> Result<(Vec<RecordBatch>, String)> {
    let mut ctx = ExecutionContext::new();
    register_table_with_tag(&mut ctx, table_id, schema, batches).await?;

    // Build a plan, and inject our row tag where needed
    let original_plan = ctx.create_logical_plan(&query)?;
    let plan = inject(&table_id, &original_plan).unwrap();

    _execute(&mut ctx, plan).await
}

/// Join two tables, returning one result and two artifacts
///
/// Note that currently the query is executed twice to produce both the left
/// and right side artifacts.
async fn join(left_id: &String, right_id: &String, left_schema: Arc<Schema>, right_schema: Arc<Schema>, left_batches: Vec<RecordBatch>, right_batches: Vec<RecordBatch>, query: String) -> Result<(Vec<RecordBatch>, (String, String))> {
    let mut ctx = ExecutionContext::new();

    // Register the left table with tags, right without
    register_table_with_tag(&mut ctx, left_id, left_schema.clone(), left_batches.clone()).await?;
    let provider = MemTable::try_new(right_schema.clone(), vec![right_batches.clone()])?;
    ctx.register_table(right_id.as_str(), Arc::new(provider))?;

    // Build a plan, and inject our row tag where needed
    let original_plan = ctx.create_logical_plan(&query)?;
    let plan = inject(&left_id, &original_plan).unwrap();

    let (result, left_artifact) = _execute(&mut ctx, plan).await?;

    // Repeat the entire query but trace the right side this time
    let mut ctx = ExecutionContext::new();

    register_table_with_tag(&mut ctx, right_id, right_schema, right_batches).await?;
    let provider = MemTable::try_new(left_schema, vec![left_batches])?;
    ctx.register_table(left_id.as_str(), Arc::new(provider))?;

    let original_plan = ctx.create_logical_plan(&query)?;
    let plan = inject(&right_id, &original_plan).unwrap();

    let (_, right_artifact) = _execute(&mut ctx, plan).await?;

    Ok((result, (left_artifact, right_artifact)))
}

async fn _execute(ctx: &mut ExecutionContext, plan: plan::LogicalPlan) -> Result<(Vec<RecordBatch>, String)> {
    // Execute the modified query
    let df = Arc::new(DataFrameImpl::new(
        ctx.state.clone(),
        &ctx.optimize(&plan).unwrap()
    ));

    let batches = df.collect().await?;

    // Split off the artifact from the result batches
    let mut artifact = Vec::new();
    let mut result = Vec::new();
    for batch in &batches {
        let schema = batch.schema();
        let i = schema.index_of("__tag__").unwrap();

        let column = batch.column(i);

        let mut arr: Vec<String> = Vec::new();
        for i in 0..column.len() {
            let tag: String = column.as_any().downcast_ref::<array::StringArray>().expect("").value(i).into();
            arr.push(tag);
        }
        artifact.extend(arr);

        let indices: Vec<usize> = (0..i).into_iter().map(|x| x as usize).collect();
        result.push(batch.project(&indices[..])?);
    }

    let artifact = format!("[{}]", artifact.join(","));

    Ok((result, artifact))
}

/// Apply an artifact to a table
///
/// Strategies:
/// 1. Reorder
///     The artifact is of depth 1, which simply unnests to a table of the same size as
///     the target table. To apply it, the artifact is joined with the target and then
///     ordered to the original order in the artifact (because joins are destructive to
///     the table order).
///
/// 2. Expansion
///     The artifact is of depth 1, but some rows may have been dropped or duplicated.
///     In practice, the same strategy may be applied as reorder because the left join
///     will automatically take care of expanding duplicate rows and dropping non-existent
///     ones.
///
/// 3. Contraction
///     The artifact is of depth N>1. An aggregate function has been applied, which has
///     grouped rows using an aggregate function. For each nested artifact, the grouped
///     tags in the artifact are joined to the target table, after which a GROUP BY is
///     applied based on the tag.
///
///     The aggregate function that has to be applied for each of the columns needs to
///     be explicitly defined.
///
///     NOTE: Order in middle aggregates is not retained
///
/// Returns the resulting batches
async fn apply_artifact(table_id: &String, schema: Arc<Schema>, batches: Vec<RecordBatch>, artifact: String) -> Result<Vec<RecordBatch>> {
    let mut ctx = ExecutionContext::new();
    register_table_with_tag(&mut ctx, table_id, schema.clone(), batches).await?;

    // Save original columns
    let select: Vec<Expr> = schema.fields().into_iter().map(|field| col(&field.name().clone())).collect();

    let batches = match artifact_to_batches(artifact)? {
        // A depth of 1 is just a simple re-order or expansion case
        (1, batches) => {
            // Build a RecordBatch out of the artifact
            let artifact_schema = batches[0].schema();

            // Register the artifact
            let provider = MemTable::try_new(artifact_schema, vec![batches])?;
            ctx.register_table("artifact", Arc::new(provider))?;

            // Join the table to the artifact
            let left = ctx.table("artifact")?;
            let right = ctx.table(table_id.as_str())?;

            let df = left.join(right, plan::JoinType::Left, &["__ltag__"], &["__tag__"])?;
            let df = df.sort(vec![col("__lsort__").sort(true, false)])?;
            let df = df.select(select)?;

            let batches = df.collect().await?;

            batches
        },

        // A depth higher than 1 means an aggregate function was applied
        (depth, batches) => {
            let artifact_schema = batches[0].schema();

            let provider = MemTable::try_new(artifact_schema, vec![batches])?;
            ctx.register_table("artifact", Arc::new(provider))?;

            let left = ctx.table("artifact")?;
            let right = ctx.table(table_id.as_str())?;

            // Each field is expected to have a preferred default aggregate function specified
            // in the metadata. Defaults to array_agg if none is found.
            let aggr_expr: Vec<Expr> = schema.fields().into_iter().map(|field| {
                let fun = match field.metadata().clone().and_then(|m| m.get("aggregate_fn").and_then(|f| Some(f.clone()))) {
                    Some(f) => f,
                    None => "array_agg".to_string()
                };

                Expr::Alias(
                    Box::new(Expr::AggregateFunction{
                        fun: aggregates::AggregateFunction::from_str(&fun).unwrap(),
                        args: vec![col(&field.name().clone())],
                        distinct: false
                    }),
                    field.name().clone()
                )
            }).collect();

            let sort_aggr: Vec<Expr> = vec![
                Expr::Alias(
                    Box::new(Expr::AggregateFunction{
                        fun: aggregates::AggregateFunction::Min,
                        args: vec![col("__lsort__")],
                        distinct: false
                    }),
                    "__lsort__".to_string()
                )
            ];

            // Join with the artifact using the fully unnested tag
            let mut df = left.join(right, plan::JoinType::Left, &["__ltag__"], &["__tag__"])?;

            // Next, the grouped tags are used to replay the aggregate functions
            for i in 1..depth {
                let group_expr: Vec<Expr> = (i+1..depth+1).rev().map(|j| col(&format!("__ltag{}__", j))).collect();

                df = df.aggregate(group_expr, [sort_aggr.clone(), aggr_expr.clone()].concat())?;
            }

            let df = df.sort(vec![col("__lsort__").sort(true, false)])?;
            let df = df.select(select)?;

            let batches = df.collect().await?;

            batches
        }
    };

    Ok(batches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use datafusion::arrow::util::pretty;
    use datafusion::arrow::datatypes::Field;

    fn gen_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false).with_metadata(Some(BTreeMap::from([
                ("aggregate_fn".to_string(), "sum".to_string())
            ]))),
            Field::new("b", DataType::Utf8, false),
            Field::new("c", DataType::Timestamp(TimeUnit::Second, None), false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(array::Int64Array::from(vec![2, 22, 20, 200])),
                Arc::new(array::StringArray::from(vec!["a", "a", "b", "b"])),
                Arc::new(array::TimestampSecondArray::from(vec![1642183923, 1642183924, 1642183925, 1642183926])),
            ],
        ).unwrap();

        batch
    }

    #[test]
    fn test_sort_artifact() {
        let table_id = "test".to_owned();
        let batch = gen_batch();
        let schema = batch.schema();

        let query = "SELECT * FROM test ORDER BY a DESC".to_owned();

        tokio_test::block_on(async move {
            // Execute a query, but discard results and save the artifact
            let (_batches, artifact) = execute(&table_id, schema.clone(), vec![batch.clone()], query).await.unwrap();

            assert_eq!(artifact, "[4,2,3,1]");

            // Apply the artifact on the original table, without the query
            let batches = apply_artifact(&table_id, schema, vec![batch], artifact).await.unwrap();

            // This should still return our expected result
            let expected = "\
                +-----+---+---------------------+\n\
                | a   | b | c                   |\n\
                +-----+---+---------------------+\n\
                | 200 | b | 2022-01-14 18:12:06 |\n\
                | 22  | a | 2022-01-14 18:12:04 |\n\
                | 20  | b | 2022-01-14 18:12:05 |\n\
                | 2   | a | 2022-01-14 18:12:03 |\n\
                +-----+---+---------------------+\
            ";
            let result: String = pretty::pretty_format_batches(&batches).unwrap().to_string();

            assert_eq!(result.as_str(), expected);
        });
    }

    #[test]
    fn test_join_artifact() {
        let left_id = "left".to_owned();
        let left_batch = gen_batch();
        let left_schema = left_batch.schema();

        let right_id = "right".to_owned();
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Utf8, false),
            Field::new("d", DataType::Int64, false),
        ]));
        let right_batch = RecordBatch::try_new(
            right_schema.clone(),
            vec![
                Arc::new(array::StringArray::from(vec!["a", "a", "c"])),
                Arc::new(array::Int64Array::from(vec![3, 30, 300])),
            ],
        ).unwrap();

        let left_query = "SELECT left.b, left.a, right.d FROM left LEFT JOIN right ON left.b = right.b ORDER BY b, a".to_owned();
        let full_query = "SELECT left.b, left.a, right.d FROM left FULL JOIN right ON left.b = right.b ORDER BY b, a".to_owned();

        tokio_test::block_on(async move {
            // First test if the left join correctly produces a left and right artifact. Everything
            // else is pretty much the same as the sort test, except for the possibility of NULL
            // rows in the right side. See the next test for more info.
            let (_, (left_artifact, right_artifact)) = join(&left_id, &right_id, left_schema.clone(), right_schema.clone(), vec![left_batch.clone()], vec![right_batch.clone()], left_query).await.unwrap();

            assert_eq!(left_artifact, "[1,1,2,2,3,4]");
            assert_eq!(right_artifact, "[1,2,1,2,,]");

            // The more difficult case is a full outer join, because it may produces NULLs in both
            // the left and right side. In practice everything is applied in the same manner, but
            // what happens behind the scenes is noteworthy.
            //
            // The result of a NULL row for either side is an empty value in the artifact due to
            // the string aggregate, which is parsed as an empty string when converting the
            // artifact to batches. This empty string will not match the artifact join and again
            // produce a NULL value in the resulting table.
            let (_, (left_artifact, right_artifact)) = join(&left_id, &right_id, left_schema.clone(), right_schema.clone(), vec![left_batch.clone()], vec![right_batch.clone()], full_query).await.unwrap();

            assert_eq!(left_artifact, "[1,1,2,2,3,4,]");
            assert_eq!(right_artifact, "[1,2,1,2,,,3]");

            // An artifact should be applied on the same counterpart
            let left_batches = apply_artifact(&left_id, left_schema, vec![left_batch], left_artifact).await.unwrap();
            let right_batches = apply_artifact(&right_id, right_schema, vec![right_batch], right_artifact).await.unwrap();

            // This should result in two halves, both having the NULL rows in the correct places
            let left_expected = "\
                +-----+---+---------------------+\n\
                | a   | b | c                   |\n\
                +-----+---+---------------------+\n\
                | 2   | a | 2022-01-14 18:12:03 |\n\
                | 2   | a | 2022-01-14 18:12:03 |\n\
                | 22  | a | 2022-01-14 18:12:04 |\n\
                | 22  | a | 2022-01-14 18:12:04 |\n\
                | 20  | b | 2022-01-14 18:12:05 |\n\
                | 200 | b | 2022-01-14 18:12:06 |\n\
                |     |   |                     |\n\
                +-----+---+---------------------+\
            ";
            let right_expected = "\
                +---+-----+\n\
                | b | d   |\n\
                +---+-----+\n\
                | a | 3   |\n\
                | a | 30  |\n\
                | a | 3   |\n\
                | a | 30  |\n\
                |   |     |\n\
                |   |     |\n\
                | c | 300 |\n\
                +---+-----+\
            ";

            let left_result: String = pretty::pretty_format_batches(&left_batches).unwrap().to_string();
            let right_result: String = pretty::pretty_format_batches(&right_batches).unwrap().to_string();

            assert_eq!(left_result.as_str(), left_expected);
            assert_eq!(right_result.as_str(), right_expected);
        });
    }

    #[test]
    fn test_agg_artifact() {
        let table_id = "test".to_owned();
        let batch = gen_batch();
        let schema = batch.schema();

        let query = "SELECT SUM(a) AS a FROM test GROUP BY b ORDER BY 1".to_owned();

        tokio_test::block_on(async move {
            let (_batches, artifact) = execute(&table_id, schema.clone(), vec![batch.clone()], query).await.unwrap();

            assert_eq!(artifact, "[[1,2],[3,4]]");

            let batches = apply_artifact(&table_id, schema, vec![batch], artifact).await.unwrap();

            let expected: array::Int64Array = [Some(24), Some(220)].iter().collect();
            let result = batches[0].column(0).as_any().downcast_ref::<array::Int64Array>().expect("");

            assert_eq!(result, &expected);
        });
    }

    #[test]
    fn downcast_rows() {
        let batch = gen_batch();
        let schema = batch.schema();

        let mut i: usize = 0;
        for column in batch.columns() {
            let row = column.slice(0, 1);
            let dtype = schema.field(i).data_type();

            if dtype == &DataType::Int64 {
                row.as_any().downcast_ref::<array::Int64Array>().expect("Int64 downcast failed");
            } else if dtype == &DataType::Utf8 {
                row.as_any().downcast_ref::<array::StringArray>().expect("Utf8 downcast failed");
            } else if dtype == &DataType::Timestamp(TimeUnit::Second, None) {
                row.as_any().downcast_ref::<array::TimestampSecondArray>().expect("Timestamp downcast failed");
            }

            i+= 1;
        }

        assert!(true);
    }
}

