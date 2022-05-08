use std::sync::Arc;

use datafusion::arrow::{array::ArrayRef, datatypes::DataType};
use datafusion::physical_plan::functions::Volatility;
use datafusion::physical_plan::udaf::AggregateUDF;
use datafusion::{error::Result, logical_plan::create_udaf, physical_plan::Accumulator};
use datafusion::{scalar::ScalarValue};

#[derive(Debug)]
struct StringAgg {
    txt: String
}

impl StringAgg {
    pub fn new() -> Self {
        StringAgg { txt: "".to_string() }
    }

    fn update(&mut self, values: &[ScalarValue]) -> Result<()> {
        let value = &values[0];
        match value {
            ScalarValue::Utf8(e) => e.clone().map(|value| {
                self.txt.push_str(&value);
                self.txt.push(',');
            }),
            _ => unreachable!(""),
        };
        Ok(())
    }

    fn merge(&mut self, states: &[ScalarValue]) -> Result<()> {
        let txt = &states[0];

        match txt {
            ScalarValue::Utf8(Some(txt)) => {
                self.txt.push_str(txt);
                self.txt.push(',');
            }
            _ => unreachable!(""),
        };
        Ok(())
    }
}

impl Accumulator for StringAgg {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::from(self.txt.as_str()),
        ])
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        let mut txt = self.txt.clone();
        txt.pop();
        Ok(ScalarValue::from(format!("[{}]", txt).as_str()))
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        };
        (0..values[0].len()).try_for_each(|index| {
            let v = values
                .iter()
                .map(|array| ScalarValue::try_from_array(array, index))
                .collect::<Result<Vec<_>>>()?;
            self.update(&v)
        })
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        };
        (0..states[0].len()).try_for_each(|index| {
            let v = states
                .iter()
                .map(|array| ScalarValue::try_from_array(array, index))
                .collect::<Result<Vec<_>>>()?;
            self.merge(&v)
        })
    }
}

pub fn string_agg_fn() -> AggregateUDF {
    create_udaf(
        "string_agg",
        DataType::Utf8,
        Arc::new(DataType::Utf8),
        Volatility::Immutable,
        Arc::new(|| Ok(Box::new(StringAgg::new()))),
        Arc::new(vec![DataType::Utf8]),
    )
}

