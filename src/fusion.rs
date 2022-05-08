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
use datafusion::arrow::datatypes::{DataType, TimeUnit, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::ipc::reader::FileReader;
use datafusion::arrow::ipc::writer::FileWriter;

use datafusion::execution::dataframe_impl::DataFrameImpl;
use datafusion::logical_plan::Expr;
use datafusion::physical_plan::window_functions::{WindowFunction, BuiltInWindowFunction};
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

                    Ok(artifact.join(",").into())
                },
                Err(_) => {
                    console::log_1(&"Error collecting dataframe".into());

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
async fn execute(table_id: &String, schema: Arc<Schema>, batches: Vec<RecordBatch>, query: String) -> Result<(Vec<RecordBatch>, Vec<String>)> {
    let mut ctx = ExecutionContext::new();
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

    Ok((result, artifact))
}


#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_artifact() {
        let table_id = "test".to_owned();
        let batch = gen_batch();
        let schema = batch.schema();

        let query = "SELECT * FROM test ORDER BY a DESC".to_owned();

        tokio_test::block_on(async move {
            let (batches, artifact) = execute(&table_id, schema, vec![batch], query).await.unwrap();

            assert_eq!(artifact, vec!["4", "2", "3", "1"]);
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

