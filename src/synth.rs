use std::sync::Arc;
use serde::{Serialize, Deserialize};
use serde_json::value::Value;
use probability::prelude::*;
use rand::rngs::OsRng;
use rand::distributions::{Distribution as RandDistribution, uniform, Uniform, WeightedIndex};
use num::cast::AsPrimitive;

use datafusion::arrow::{
    array,
    array::ArrayRef,
    datatypes,
    datatypes::{ArrowPrimitiveType, ArrowNativeType, DataType, TimeUnit, Schema},
    record_batch::RecordBatch,
};
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::logical_plan::{floor, abs, Expr};
use datafusion::scalar::ScalarValue;
use datafusion::prelude::*;

struct Source<T>(T);

impl<T: rand::RngCore> source::Source for Source<T> {
    fn read_u64(&mut self) -> u64 {
        self.0.next_u64()
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Distribution {
    bins: Value,
    probabilities: Vec<f64>
}

#[derive(Serialize, Deserialize, Debug)]
struct ColumnDescription {
    name: String,
    data_type: DataType,
    min: Value,
    max: Value,
    distribution: Distribution
}

impl ColumnDescription {
    fn new(name: &String, data_type: &DataType, min: Value, max: Value, distribution: Distribution) -> Self {
        ColumnDescription { name: name.clone(), data_type: data_type.clone(), min: min, max: max, distribution: distribution }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TableDescription {
    num_rows: usize,
    attributes: Vec<String>,
    descriptions: Vec<ColumnDescription>
}

pub async fn describe(table_id: &String, schema: Arc<Schema>, batches: Vec<RecordBatch>, nr_bins: Option<usize>) -> Result<TableDescription> {
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

            to_value(field.data_type(), result[0].column(0).clone())
        };

        let max = {
            let result = ctx.table(table_id.as_str())?
                .aggregate(vec![], vec![max(col(field.name()))])?
                .collect().await?;

            to_value(field.data_type(), result[0].column(0).clone())
        };

        let bins: Distribution = {
            if is_numeric(field.data_type()) {
                let nr_bins = nr_bins.unwrap_or(20);
                let min_val: f64 = serde_json::from_value(min.clone()).unwrap();
                let max_val: f64 = serde_json::from_value(max.clone()).unwrap();
                let bin_size = (max_val - min_val) / (nr_bins as f64);

                if bin_size != 0.0 {
                    let min_expr = Expr::Literal(ScalarValue::Float64(Some(min_val + (f32::EPSILON) as f64)));
                    let bin_expr = Expr::Literal(ScalarValue::Float64(Some(bin_size)));

                    // select count(*)
                    // from <table>
                    // where <field> is not null
                    // group by floor(abs(<field> - (<min>+<epsilon>)) / <bin_size>)
                    // order by <field>
                    let result = ctx.table(table_id.as_str())?
                        .filter(col(field.name()).is_not_null())?
                        .aggregate(vec![
                            Expr::Alias(
                                Box::new(floor(abs(col(field.name()) - min_expr.clone()) / bin_expr.clone())),
                                "group_by".to_string()
                            )], vec![
                            Expr::Alias(
                                Box::new(count(col(field.name()))),
                                "n".to_string()
                            )])?
                        .sort(vec![col("group_by").sort(true, true)])?
                        .limit(nr_bins)?
                        .collect().await?;

                    let bins = result[0].column(0).as_any().downcast_ref::<array::Float64Array>().expect("").values();
                    let counts = result[0].column(1).as_any().downcast_ref::<array::UInt64Array>().expect("");

                    let freq = (0..nr_bins).map(|bin| {
                        // The group by will not contain empty bins
                        match bins.iter().position(|x| x == &(bin as f64)) {
                            Some(index) => counts.value(index),
                            None => 0
                        }
                    }).collect::<Vec<u64>>();
                    let total = freq.iter().sum::<u64>() as f64;

                    Distribution {
                        bins: (0..nr_bins).map(|b| min_val + bin_size * (b as f64)).collect::<Vec<f64>>().into(),
                        probabilities: freq.into_iter().map(|f| (f as f64) / total).collect::<Vec<f64>>().into()
                    }

                } else {
                    Distribution {
                        bins: Vec::<f64>::new().into(),
                        probabilities: Vec::<f64>::new().into()
                    }
                }

            // Assume all strings are categorical for now
            } else if field.data_type() == &DataType::Utf8 {
                let result = ctx.table(table_id.as_str())?
                    // TODO: handle case where all rows are null
                    .filter(col(field.name()).is_not_null())?
                    .aggregate(vec![
                        Expr::Alias(
                            Box::new(col(field.name())),
                            "group_by".to_string()
                        )], vec![
                        Expr::Alias(
                            Box::new(count(col(field.name()))),
                            "n".to_string()
                        )])?
                    .sort(vec![col("group_by").sort(true, true)])?
                    .collect().await?;

                let bins = result[0].column(0).as_any().downcast_ref::<array::StringArray>().expect("");
                let counts = result[0].column(1).as_any().downcast_ref::<array::UInt64Array>().expect("");

                let dist = (0..counts.len()).map(|i| {
                    (bins.value(i).to_string(), counts.value(i))
                }).collect::<Vec<(String, u64)>>();
                let total = dist.iter().map(|d| d.1).sum::<u64>() as f64;

                Distribution {
                    bins: dist.iter().map(|d| d.0.clone()).collect::<Vec<String>>().into(),
                    probabilities: dist.into_iter().map(|d| (d.1 as f64) / total).collect::<Vec<f64>>().into()
                }

            } else {
                Distribution {
                    bins: Vec::<String>::new().into(),
                    probabilities: Vec::<f64>::new().into()
                }
            }
        };

        descriptions.push(ColumnDescription::new(field.name(), field.data_type(), min, max, bins));
    };

    Ok(TableDescription {
        num_rows: num_rows,
        attributes: attributes,
        descriptions: descriptions
    })
}

pub fn add_laplace_noise(description: TableDescription, epsilon: f64) -> TableDescription {
    let num_rows = description.num_rows;
    let nr_attributes = description.attributes.len();
    let mut source = Source(OsRng);

    let descriptions = description.descriptions.into_iter().map(|column| {
        let probabilities = column.distribution.probabilities.clone();

        let sensitivity = 2. / num_rows as f64;
        let budget = epsilon / nr_attributes as f64;
        let scale = sensitivity / budget;

        let distribution = Laplace::new(0., scale);
        let mut sampler = Independent(&distribution, &mut source);

        let noisy_probabilities = probabilities.iter().map(|p| p + sampler.next().unwrap()).collect::<Vec<f64>>();
        let normalized_probabilities = if probabilities.len() > 0 {
            let min = noisy_probabilities.iter().min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)).unwrap().clone();
            let diff = if min < 0. { min.abs() } else { 0. };
            let total = noisy_probabilities.iter().map(|p| p + diff).sum::<f64>();

            noisy_probabilities.into_iter().map(|p| (p + diff) / total).collect::<Vec<f64>>()
        } else {
            probabilities
        };

        ColumnDescription {
            name: column.name,
            data_type: column.data_type,
            min: column.min,
            max: column.max,
            distribution: Distribution {
                bins: column.distribution.bins,
                probabilities: normalized_probabilities
            }
        }
    }).collect::<Vec<ColumnDescription>>();

    TableDescription {
        num_rows: description.num_rows,
        attributes: description.attributes,
        descriptions: descriptions
    }
}

pub fn gen_synthethic_dataset(description: TableDescription) -> Vec<ArrayRef> {
    let mut rng = OsRng;
    let num_rows = description.num_rows;

    description.descriptions.into_iter().map(|column| {
        if column.data_type == DataType::Int32 {
            build_numerical_array::<i32, datatypes::Int32Type>(num_rows, column, &mut rng)
        } else if column.data_type == DataType::Int64 {
            build_numerical_array::<i64, datatypes::Int64Type>(num_rows, column, &mut rng)
        } else if column.data_type == DataType::Float32 {
            build_numerical_array::<f32, datatypes::Float32Type>(num_rows, column, &mut rng)
        } else if column.data_type == DataType::Float64 {
            build_numerical_array::<f64, datatypes::Float64Type>(num_rows, column, &mut rng)
        } else if column.data_type == DataType::Utf8 {
            build_categorical_array(num_rows, column, &mut rng)
        } else {
            build_empty_array(num_rows)
        }
    }).collect::<Vec<ArrayRef>>()
}

fn build_numerical_array<T, U: ArrowPrimitiveType<Native = T>>(num_rows: usize, column: ColumnDescription, rng: &mut OsRng) -> ArrayRef
where T: uniform::SampleUniform + for<'de> serde::Deserialize<'de> + AsPrimitive<T> + ArrowNativeType, f64: AsPrimitive<T> {
    let max: T = serde_json::from_value::<f64>(column.max).unwrap().as_();
    let bins: Vec<f64> = serde_json::from_value(column.distribution.bins).unwrap();
    let indices: Vec<usize> = (0..bins.len()).collect();

    let mut builder = array::PrimitiveBuilder::<U>::new(num_rows);

    if bins.len() > 0 {
        let dist = WeightedIndex::new(column.distribution.probabilities).unwrap();
        let index = (0..num_rows).map(|_| indices[dist.sample(rng)]).collect::<Vec<usize>>();

        for i in index {
            let start: T = bins[i].as_();
            let end = if i == bins.len()-1 { max } else { bins[i+1].as_() };
            let uniform = Uniform::from(start..end);
            let value = uniform.sample(rng) as T;

            builder.append_value(value).unwrap();
        }
        let array = builder.finish();

        Arc::new(array)
    } else {
        for _ in 0..num_rows {
            builder.append_null().unwrap();
        }
        Arc::new(builder.finish())
    }
}

fn build_categorical_array(num_rows: usize, column: ColumnDescription, rng: &mut OsRng) -> ArrayRef {
    let bins: Vec<String> = serde_json::from_value(column.distribution.bins).unwrap();
    let indices: Vec<usize> = (0..bins.len()).collect();

    let mut builder = array::StringBuilder::new(num_rows);

    if bins.len() == 0 {
        for _ in 0..num_rows {
            builder.append_null().unwrap();
        }

        Arc::new(builder.finish())

    } else if bins.len() == 1 {
        for _ in 0..num_rows {
            builder.append_value(bins[0].clone()).unwrap();
        }

        Arc::new(builder.finish())

    } else {
        let dist = WeightedIndex::new(column.distribution.probabilities).unwrap();
        let index = (0..num_rows).map(|_| indices[dist.sample(rng)]).collect::<Vec<usize>>();

        for i in index {
            builder.append_value(bins[i].clone()).unwrap();
        }

        Arc::new(builder.finish())
    }
}

fn build_empty_array(num_rows: usize) -> ArrayRef {
    Arc::new(array::NullArray::new(num_rows))
}

fn is_numeric(data_type: &DataType) -> bool {
    data_type == &DataType::Int32 ||
    data_type == &DataType::Int64 ||
    data_type == &DataType::Float32 ||
    data_type == &DataType::Float64
}

fn to_value(data_type: &DataType, value_array: ArrayRef) -> Value {
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
            let result = describe(&table_id, schema.clone(), vec![batch.clone()], Some(2)).await.unwrap();

            let json = format!("{}", json!(result));
            let expected = "{\"num_rows\":4,\"attributes\":[\"a\",\"b\"],\"descriptions\":[{\"name\":\"a\",\"data_type\":\"Int32\",\"min\":2,\"max\":200,\"distribution\":{\"bins\":[2.0,101.0],\"probabilities\":[0.75,0.25]}},{\"name\":\"b\",\"data_type\":\"Utf8\",\"min\":\"a\",\"max\":\"d\",\"distribution\":{\"bins\":[\"a\",\"b\",\"c\",\"d\"],\"probabilities\":[0.25,0.25,0.25,0.25]}}]}";

            assert_eq!(json, expected);
        });
    }

    #[test]
    fn test_laplace() {
        let table_id = "test".to_owned();
        let batch = gen_batch();
        let schema = batch.schema();

        tokio_test::block_on(async move {
            let description = add_laplace_noise(describe(&table_id, schema.clone(), vec![batch.clone()], Some(2)).await.unwrap(), 0.1);

            for c in description.descriptions {
                let total = c.distribution.probabilities.iter().sum::<f64>();
                assert_eq!(total, 1.0);
            }
        });
    }

    #[test]
    fn test_synthetic_dataset() {
        let data = r#"
            {
              "num_rows": 6,
              "attributes": [
                "bb5dec91-56de-4cf5-9051-b985fe8dac4e",
                "3522934b-2274-45df-b027-1258119759d4",
                "26375c09-4f20-4ae6-b14f-40a2846e2744"
              ],
              "descriptions": [
                {
                  "name": "bb5dec91-56de-4cf5-9051-b985fe8dac4e",
                  "data_type": "Utf8",
                  "min": "2022-12-19T21:31:15+01:00",
                  "max": "2022-12-19T21:31:15+01:00",
                  "distribution": {
                    "bins": [
                      "2022-12-19T21:31:15+01:00"
                    ],
                    "probabilities": [
                      1
                    ]
                  }
                },
                {
                  "name": "3522934b-2274-45df-b027-1258119759d4",
                  "data_type": "Utf8",
                  "min": "A",
                  "max": "B",
                  "distribution": {
                    "bins": [
                      "A",
                      "B"
                    ],
                    "probabilities": [
                      0.3333333333333333,
                      0.6666666666666666
                    ]
                  }
                },
                {
                  "name": "26375c09-4f20-4ae6-b14f-40a2846e2744",
                  "data_type": "Int64",
                  "min": 0,
                  "max": 0,
                  "distribution": {
                    "bins": [],
                    "probabilities": []
                  }
                }
              ]
            }"#;

        let description: TableDescription = serde_json::from_str(data).unwrap();

        tokio_test::block_on(async move {
            let arrays = gen_synthethic_dataset(add_laplace_noise(description, 0.1));

            assert_eq!(arrays.len(), 3);
        });
    }
}
