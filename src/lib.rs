extern crate console_error_panic_hook;

use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::*;
use web_sys::console;
use futures_channel::oneshot;
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

use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::prelude::*;

#[cfg(feature = "wee_alloc")]
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;


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

                JsValue::from_serde(&schema).unwrap()
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

    pub fn clone_table(&self, table_id: String) -> String {
        let clone_id = Uuid::new_v4().to_hyphenated().to_string();

        let batches  = self.inner.tables.borrow().get(&table_id).unwrap().batches.clone();
        self.inner.tables.borrow_mut().insert(clone_id.clone(), Table { batches: batches });

        clone_id
    }

    pub fn query(&self, table_id: String, query: String) -> Promise {
        let _self = self.inner.clone();

        let batches = _self.tables.borrow().get(&table_id).unwrap().batches.clone();
        let schema = batches[0].schema();

        wasm_bindgen_futures::future_to_promise(async move {
            let df = execute(&table_id, schema, batches, query).unwrap();
            match df.collect().await {
                Ok(batches) => {
                    _self.tables.borrow_mut().insert(table_id, Table { batches: batches });

                    Ok(JsValue::undefined())
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

fn execute(table_id: &String, schema: Arc<Schema>, batches: Vec<RecordBatch>, query: String) -> Result<Arc<dyn DataFrame>> {
    let mut ctx = ExecutionContext::new();
    let provider = MemTable::try_new(schema, vec![batches])?;
    ctx.register_table(table_id.as_str(), Arc::new(provider))?;

    let df = ctx.sql(&query)?;

    Ok(df)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn downcast_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
            Field::new("c", DataType::Timestamp(TimeUnit::Second, None), false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(array::Int64Array::from(vec![2, 20, 20, 200])),
                Arc::new(array::StringArray::from(vec!["a", "b", "c", "d"])),
                Arc::new(array::TimestampSecondArray::from(vec![1642183923, 1642183924, 1642183925, 1642183926])),
            ],
        ).unwrap();

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

