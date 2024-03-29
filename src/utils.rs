use datafusion::arrow::array;
use datafusion::arrow::array::{Array, ArrayRef, ArrayData};
use datafusion::arrow::datatypes::{DataType, Field};


macro_rules! as_string_array {
    ($param:ident, $data_type:ty) => {
        {
            let array = $param.as_any().downcast_ref::<$data_type>().expect("");
            let mut values: Vec<String> = Vec::new();
            for i in 0..$param.len() {
                values.push(array.value(i).to_string())
            }

            values.join(",")
        }
    }
}

pub fn to_string_array(data_type: DataType, array_ref: ArrayRef) -> String {
    match data_type {
        DataType::Int32 => as_string_array!(array_ref, array::Int32Array),
        DataType::Int64 => as_string_array!(array_ref, array::Int64Array),
        DataType::Float32 => as_string_array!(array_ref, array::Float32Array),
        DataType::Float64 => as_string_array!(array_ref, array::Float64Array),
        DataType::Boolean => as_string_array!(array_ref, array::BooleanArray),
        DataType::Utf8 => as_string_array!(array_ref, array::StringArray),
        _ => panic!("Not implemented: string array")
    }
}

/*
 * Drop metadata from fields in nested datatypes
 *
 * When loading data from parquet, it may have metadata like: `Some({"PARQUET:field_id": "3"})`.
 * This is problematic when trying to merge or alter schemas, because the data types won't match.
 * Because this metadata is pointless at this stage, we just drop it.
 *
 * Note that this drops all metadata.
 */
pub fn discard_nested_metadata(array_ref: &ArrayRef) -> ArrayRef {
    match array_ref.data_type() {
        // TODO: handle nested lists
        DataType::List(field) => {
            let data = array_ref.data();

            // Take all the original fields, except for datatype
            ArrayRef::from(ArrayData::try_new(
                DataType::List(Box::new(Field::new(field.name(), field.data_type().clone(), field.is_nullable()))),
                data.len(),
                Some(data.null_count()),
                data.null_buffer().and_then(|b| Some(b.clone())),
                data.offset(),
                data.buffers().to_vec(),
                data.child_data().to_vec()
            ).unwrap())
        },
        _ => array_ref.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use datafusion::arrow::array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;


    fn gen_list_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::List(Box::new(Field::new("item", DataType::Int32, true))), false),
            Field::new("b", DataType::List(Box::new(Field::new("item", DataType::Utf8, true))), false)
        ]));

        let a_item_builder = array::Int32Array::builder(1);
        let b_item_builder = array::StringBuilder::new(1);

        let mut a_builder = array::ListBuilder::with_capacity(a_item_builder, 1);
        let mut b_builder = array::ListBuilder::with_capacity(b_item_builder, 4);

        a_builder.values().append_value(1).unwrap();
        a_builder.values().append_value(2).unwrap();
        a_builder.values().append_value(3).unwrap();
        a_builder.values().append_value(4).unwrap();
        a_builder.append(true).unwrap();

        b_builder.values().append_value("a").unwrap();
        b_builder.values().append_value("b").unwrap();
        b_builder.values().append_value("c").unwrap();
        b_builder.values().append_value("d").unwrap();
        b_builder.append(true).unwrap();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(a_builder.finish()),
                Arc::new(b_builder.finish())
            ],
        ).unwrap();

        batch
    }

    #[test]
    fn test_utils() {
        let batch = gen_list_batch();
        let schema = batch.schema();

        let mut results: Vec<String> = Vec::new();
        let expected = vec!["1,2,3,4", "a,b,c,d"];

        for column in batch.columns() {
            let result = match schema.field(0).data_type() {
                DataType::List(_) => {
                    let list = column.as_any().downcast_ref::<array::ListArray>().expect("");
                    to_string_array(list.value_type(), list.value(0))
                },
                _ => "".to_string()
            };

            results.push(result);
        }

        assert_eq!(expected, results);
    }
}
