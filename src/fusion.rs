extern crate console_error_panic_hook;

use wasm_bindgen::prelude::*;
use web_sys::console;
use js_sys::{Promise, Object, Uint8Array};

use std::io::Cursor;
use std::sync::Arc;
use std::cell::RefCell;
use std::collections::hash_map::HashMap;

use uuid::Uuid;

use datafusion::arrow::array;
use datafusion::arrow::datatypes::{DataType, TimeUnit, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::ipc::reader::FileReader;
use datafusion::arrow::ipc::writer::FileWriter;

use datafusion::logical_plan::{plan, Expr};
use datafusion::execution::dataframe_impl::DataFrameImpl;
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::prelude::*;

use crate::tag::*;

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
            None => panic!("Invalid table id")
        }
    }

    pub fn get_schema(&self, table_id: String) -> JsValue {
        match self.inner.tables.borrow().get(&table_id) {
            Some(table) => {
                let batch = &table.batches[0];
                let schema = batch.schema();

                JsValue::from_serde(&schema.to_json()).unwrap()
            },
            None => panic!("Invalid table id")
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

                let batch = &table.batches[batch_id];
                let schema = batch.schema();

                let mut i: usize = 0;
                for column in batch.columns() {
                    let row = column.slice(row_id - rows, 1);

                    let key: JsValue = schema.field(i).name().into();
                    let value: JsValue = match schema.field(i).data_type() {
                        DataType::Int32 => row.as_any().downcast_ref::<array::Int32Array>().expect("").value(0).into(),
                        DataType::Float64 => row.as_any().downcast_ref::<array::Float64Array>().expect("").value(0).into(),
                        DataType::Utf8 => row.as_any().downcast_ref::<array::StringArray>().expect("").value(0).into(),
                        DataType::Boolean => row.as_any().downcast_ref::<array::BooleanArray>().expect("").value(0).into(),
                        DataType::Timestamp(TimeUnit::Second, None) => row.as_any().downcast_ref::<array::TimestampSecondArray>().expect("").value(0).into(),

                        // Gets cast to BigInt, which is not that common. Just force Number instead
                        DataType::Int64 => (row.as_any().downcast_ref::<array::Int64Array>().expect("").value(0) as f64).into(),
                        _ => panic!("Unsupported data type")
                    };

                    js_sys::Reflect::set(&obj, &key, &value).unwrap();

                    i += 1;
                }

            },
            None => {}
        }

        obj
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
            self.inner.tables.borrow_mut().insert(id.clone(), Table { batches: batches });
        }

        id
    }

    pub fn clone_table(&self, table_id: String, clone_id: String) -> String {
        let id = if clone_id.is_empty() {
            Uuid::new_v4().to_hyphenated().to_string()
        } else {
            clone_id
        };

        let batches  = self.inner.tables.borrow().get(&table_id).unwrap().batches.clone();
        self.inner.tables.borrow_mut().insert(id.clone(), Table { batches: batches });

        id
    }

    pub fn drop_table(&self, table_id: String) {
        self.inner.tables.borrow_mut().remove(&table_id);
    }

    pub fn query(&self, table_id: String, query: String) -> Promise {
        let _self = self.inner.clone();

        let batches = _self.tables.borrow().get(&table_id).unwrap().batches.clone();
        let schema = batches[0].schema();

        wasm_bindgen_futures::future_to_promise(async move {
            let batches = execute(&table_id, schema, batches, query).await;
            match batches {
                Ok((batches, artifact)) => {
                    _self.tables.borrow_mut().insert(table_id, Table { batches: batches });

                    Ok(artifact.into())
                },
                Err(_) => {
                    console::log_1(&"Error collecting dataframe".into());

                    Err(JsValue::undefined())
                },
            }
        })
    }

    pub fn apply_artifact(&self, table_id: String, artifact: String) -> Promise {
        let _self = self.inner.clone();

        let batches = _self.tables.borrow().get(&table_id).unwrap().batches.clone();
        let schema = batches[0].schema();

        wasm_bindgen_futures::future_to_promise(async move {
            let batches = apply_artifact(&table_id, schema, batches, artifact).await;
            match batches {
                Ok(batches) => {
                    _self.tables.borrow_mut().insert(table_id, Table { batches: batches });

                    Ok(JsValue::undefined())
                },
                Err(_) => {
                    console::log_1(&"Error applying artifact".into());

                    Err(JsValue::undefined())
                },
            }
        })
    }

    pub fn export_table(&self, table_id: String) -> Uint8Array {
        let batches  = self.inner.tables.borrow().get(&table_id).unwrap().batches.clone();
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
    // Save original columns
    let select: Vec<Expr> = schema.fields().into_iter().map(|field| col(&field.name().clone())).collect();

    let mut ctx = ExecutionContext::new();
    register_table_with_tag(&mut ctx, table_id, schema, batches).await?;

    let artifact_depth = {
        let mut i = 0;
        for c in artifact.as_str().chars() {
            if c != '[' {
                break;
            }
            i += 1;
        }
        i
    };

    // A depth of 1 is just a simple re-order or expansion case
    let batches = if artifact_depth == 1 {
        // Build a RecordBatch out of the artifact
        let batches = artifact_to_batches(artifact)?;
        let schema = batches[0].schema();

        // Register the artifact
        let provider = MemTable::try_new(schema, vec![batches])?;
        ctx.register_table("artifact", Arc::new(provider))?;

        // Join the table to the artifact
        let left = ctx.table("artifact")?;
        let right = ctx.table(table_id.as_str())?;

        let df = left.join(right, plan::JoinType::Left, &["__ltag__"], &["__tag__"])?;
        let df = df.sort(vec![col("__lsort__").sort(true, false)])?;
        let df = df.select(select)?;

        let batches = df.collect().await?;

        batches

    // A depth higher than 1 means an aggregate function was applied
    } else if artifact_depth > 1 {
        // TODO
        vec![]

    } else {
        unreachable!("");
    };

    Ok(batches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::util::pretty;
    use datafusion::arrow::datatypes::Field;

    fn gen_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
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
    fn test_agg_artifact() {
        let table_id = "test".to_owned();
        let batch = gen_batch();
        let schema = batch.schema();

        let query = "SELECT SUM(a) FROM test GROUP BY b ORDER BY 1".to_owned();

        tokio_test::block_on(async move {
            let (_batches, artifact) = execute(&table_id, schema, vec![batch], query).await.unwrap();

            assert_eq!(artifact, "[[1,2],[3,4]]");
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

