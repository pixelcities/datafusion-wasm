extern crate console_error_panic_hook;

use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::*;
use web_sys::console;
use futures_channel::oneshot;
use js_sys::{Promise, Object};

use std::io::Cursor;
use std::sync::Arc;
use std::cell::RefCell;
use std::collections::hash_map::HashMap;

use uuid::Uuid;

use datafusion::arrow::array::{Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::ipc::reader::FileReader;

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

    pub fn nr_tables(&self) -> usize {
        self.inner.tables.borrow_mut().len()
    }

    pub fn get_row(&self, table_id: String, row_id: usize) -> Object {
        let obj = js_sys::Object::new();

        match self.inner.tables.borrow_mut().get(&table_id) {
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
                    let arr = row
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("Failed to downcast");

                    let key: JsValue = schema.field(i).name().into();
                    let value: JsValue = arr.value(0).into();

                    js_sys::Reflect::set(&obj, &key, &value).unwrap();

                    i += 1;
                }

            },
            None => {}
        }

        obj
    }

    pub fn load_table(&self, table: &[u8]) -> String {
        let id = Uuid::new_v4().to_hyphenated().to_string();

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

    pub fn gen_table(&self) -> Promise {
        let _self = self.inner.clone();
        let id = Uuid::new_v4().to_hyphenated().to_string();

        let (tx, rx) = oneshot::channel();

        spawn_local(async move {
            let df = ds().unwrap();
            match df.collect().await {
                Ok(batches) => {
                    if batches.len() > 0 {
                        // just store raw batches because we do not always need MemTable
                        _self.tables.borrow_mut().insert(id.clone(), Table { batches: batches });
                    }
                },
                Err(_) => {
                    console::log_1(&"Error collecting dataframe".into());
                },
            };
            drop(tx.send(id));
        });

        let done = async move {
            match rx.await {
                Ok(table_id) => Ok(table_id.into()),
                Err(_) => Err(JsValue::undefined()),
            }
        };

        wasm_bindgen_futures::future_to_promise(done)
    }
}

fn ds() -> Result<Arc<dyn DataFrame>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![2, 20, 20, 200])),
            Arc::new(Int64Array::from(vec![1, 10, 10, 100])),
        ],
    )?;

    let mut ctx = ExecutionContext::new();
    let provider = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("t", Arc::new(provider))?;

    let df = ctx.sql("SELECT a, b FROM t WHERE b >= 10")?;

    Ok(df)
}

