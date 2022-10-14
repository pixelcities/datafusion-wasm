use std::sync::Arc;
use serde::{Serialize, Deserialize};
use serde_json::value::Value;

use datafusion::arrow::array;
use datafusion::arrow::datatypes::{DataType, TimeUnit, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::prelude::*;


#[derive(Serialize, Deserialize, Debug)]
struct ColumnDescription {
    name: String,
    data_type: DataType,
    min: Value,
    max: Value
}

impl ColumnDescription {
    fn new(name: &String, data_type: &DataType, min: array::ArrayRef, max: array::ArrayRef) -> Self {
        let min_value = to_value(data_type, min);
        let max_value = to_value(data_type, max);

        ColumnDescription { name: name.clone(), data_type: data_type.clone(), min: min_value, max: max_value }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TableDescription {
    num_rows: usize,
    attributes: Vec<String>,
    descriptions: Vec<ColumnDescription>
}

pub async fn describe(table_id: &String, schema: Arc<Schema>, batches: Vec<RecordBatch>) -> Result<TableDescription> {
    let mut ctx = ExecutionContext::new();
    let provider = MemTable::try_new(schema.clone(), vec![batches.clone()])?;

    let attributes: Vec<String> = schema.clone().fields().into_iter().map(|field| field.name().clone()).collect();
    let num_rows = {
        let mut rows: usize = 0;

        for batch in batches {
            rows += batch.num_rows();
        }

        rows
    };

    ctx.register_table(table_id.as_str(), Arc::new(provider))?;

    let mut descriptions: Vec<ColumnDescription> = Vec::new();
    for field in schema.fields() {
        let min = {
            let result = ctx.table(table_id.as_str())?
                .aggregate(vec![], vec![min(col(field.name()))])?
                .collect().await?;

            result[0].column(0).clone()
        };

        let max = {
            let result = ctx.table(table_id.as_str())?
                .aggregate(vec![], vec![max(col(field.name()))])?
                .collect().await?;

            result[0].column(0).clone()
        };

        descriptions.push(ColumnDescription::new(field.name(), field.data_type(), min, max));
    };

    Ok(TableDescription {
        num_rows: num_rows,
        attributes: attributes,
        descriptions: descriptions
    })
}

fn to_value(data_type: &DataType, value_array: array::ArrayRef) -> Value {
    match data_type {
        DataType::Int32 => value_array.as_any().downcast_ref::<array::Int32Array>().expect("").value(0).into(),
        DataType::Float64 => value_array.as_any().downcast_ref::<array::Float64Array>().expect("").value(0).into(),
        DataType::Utf8 => value_array.as_any().downcast_ref::<array::StringArray>().expect("").value(0).into(),
        DataType::Boolean => value_array.as_any().downcast_ref::<array::BooleanArray>().expect("").value(0).into(),
        DataType::Timestamp(TimeUnit::Second, None) => value_array.as_any().downcast_ref::<array::TimestampSecondArray>().expect("").value(0).into(),
        DataType::Int64 => (value_array.as_any().downcast_ref::<array::Int64Array>().expect("").value(0) as f64).into(),
        _ => panic!("Unsupported data type")
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Field;
    use serde_json::json;

    fn gen_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false)
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(array::Int32Array::from(vec![2, 22, 20, 200])),
                Arc::new(array::StringArray::from(vec!["a", "b", "c", "d"]))
            ],
        ).unwrap();

        batch
    }

    #[test]
    fn test_describe() {
        let table_id = "test".to_owned();
        let batch = gen_batch();
        let schema = batch.schema();

        tokio_test::block_on(async move {
            let result = describe(&table_id, schema.clone(), vec![batch.clone()]).await.unwrap();

            let json = format!("{}", json!(result));
            let expected = "{\"num_rows\":4,\"attributes\":[\"a\",\"b\"],\"descriptions\":[{\"name\":\"a\",\"data_type\":\"Int32\",\"min\":2,\"max\":200},{\"name\":\"b\",\"data_type\":\"Utf8\",\"min\":\"a\",\"max\":\"d\"}]}";

            assert_eq!(json, expected);
        });
    }
}