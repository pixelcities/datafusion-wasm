use std::error::Error;
use std::io::Cursor;
use std::sync::Arc;

use csv_sniffer::{
    metadata::{Header, Quote},
    SampleSize,
    Sniffer
};
use datafusion::arrow::{
    array,
    array::ArrayRef,
    datatypes::{DataType, Field, Schema},
    csv::{ReaderBuilder},
    record_batch::RecordBatch
};
use calamine::{
    Reader,
    open_workbook_auto_from_rs,
    DataType as SheetType
};


pub fn parse_csv(data: &[u8]) -> Result<Vec<RecordBatch>, Box<dyn Error>> {
    let metadata = Sniffer::new()
        .header(Header { has_header_row: true, num_preamble_rows: 0 })
        .sample_size(SampleSize::All)
        .sniff_reader(Cursor::new(data))?;

    let builder = ReaderBuilder::new()
        .has_header(true)
        .with_delimiter(metadata.dialect.delimiter)
        .infer_schema(Some(100));

    let builder = match metadata.dialect.quote {
        Quote::Some(quote) => builder.with_quote(quote),
        Quote::None => builder
    };

    let reader = builder.build(Cursor::new(data))?;
    let batches: Vec<RecordBatch> = reader
        .map(|b| b.unwrap())
        .collect();

    Ok(batches)
}


fn to_type(data_type: SheetType) -> DataType {
    match data_type {
        SheetType::Empty => DataType::Null,
        SheetType::String(_) | SheetType::DateTimeIso(_) | SheetType::DurationIso(_) => DataType::Utf8,
        SheetType::Float(_) | SheetType::DateTime(_) | SheetType::Duration(_) => DataType::Float64,
        SheetType::Int(_) => DataType::Int64,
        SheetType::Bool(_) => DataType::Boolean,
        SheetType::Error(_) => DataType::Null
    }
}


pub fn parse_sheet(data: &[u8]) -> Result<Vec<RecordBatch>, Box<dyn Error>> {
    let mut workbook = open_workbook_auto_from_rs(Cursor::new(data))?;

    let sheets = workbook.sheet_names().to_owned();
    let range = workbook.worksheet_range(&sheets[0]).unwrap().unwrap();

    let mut arrays: Vec<ArrayRef> = Vec::new();
    let mut fields: Vec<Field> = Vec::new();

    let size = range.get_size();
    for column in 0..(size.1 as u32) {
        let mut nullable = false;
        let mut column_type = DataType::Null;

        // First pass, ensure the types are consistent
        for row in 1..(size.0 as u32) {
            let value = range.get_value((row, column));
            let value_type = to_type(value.unwrap_or(&SheetType::Empty).clone());

            if value.is_none() {
                nullable = true;
            }

            if column_type == DataType::Null || column_type == value_type {
                column_type = value_type;

            // If there are mixed types, default to string
            } else {
                column_type = DataType::Utf8;
                break;
            };
        }

        // Add the header to the schema
        let column_name = range.get_value((0, column)).map_or("".to_string(), |x| x.as_string().unwrap_or("".to_string()));
        fields.push(Field::new(&column_name, column_type.clone(), nullable));

        // Build the actual array
        let array: ArrayRef = match column_type {
            DataType::Utf8 => {
                let mut builder = array::StringBuilder::new(size.0 - 1);

                for row in 1..(size.0 as u32) {
                    match range.get_value((row, column)) {
                        Some(value) => builder.append_value(value.as_string().unwrap_or("".to_string())).unwrap(),
                        None => builder.append_null().unwrap()
                    }
                }
                Arc::new(builder.finish())
            },
            DataType::Float64 => {
                let mut builder = array::Float64Builder::new(size.0 - 1);

                for row in 1..(size.0 as u32) {
                    match range.get_value((row, column)) {
                        Some(value) => {
                            match value.get_float() {
                                Some(f) => builder.append_value(f).unwrap(),
                                None => builder.append_null().unwrap()
                            }
                        },
                        None => builder.append_null().unwrap()
                    }
                }
                Arc::new(builder.finish())
            },
            DataType::Int64 => {
                let mut builder = array::Int64Builder::new(size.0 - 1);

                for row in 1..(size.0 as u32) {
                    match range.get_value((row, column)) {
                        Some(value) => {
                            match value.get_int() {
                                Some(f) => builder.append_value(f).unwrap(),
                                None => builder.append_null().unwrap()
                            }
                        },
                        None => builder.append_null().unwrap()
                    }
                }
                Arc::new(builder.finish())
            },
            DataType::Boolean => {
                let mut builder = array::BooleanBuilder::new(size.0 - 1);

                for row in 1..(size.0 as u32) {
                    match range.get_value((row, column)) {
                        Some(value) => {
                            match value.get_bool() {
                                Some(f) => builder.append_value(f).unwrap(),
                                None => builder.append_null().unwrap()
                            }
                        },
                        None => builder.append_null().unwrap()
                    }
                }
                Arc::new(builder.finish())
            },
            DataType::Null | _ => {
                let mut builder = array::StringBuilder::new(size.0 - 1);
                for _ in 1..(size.0 as u32) {
                    builder.append_null().unwrap();
                }
                Arc::new(builder.finish())
            }
        };

        arrays.push(array);
    }

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        arrays
    )?;

    Ok(vec![batch])
}


#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::util::pretty;
    use base64::{Engine as _, engine::general_purpose};

    fn csv_with_comma() -> &'static [u8] {
        "\
            a,b,c\n\
            1,1.1,x\n\
            2,2.2,y\n\
            3,3.3,z\
        ".as_bytes()
    }

    fn csv_with_semicolon() -> &'static [u8] {
        "\
            a;b;c\n\
            1;1.1;x\n\
            2;2.2;y\n\
            3;3.3;z\
        ".as_bytes()
    }

    fn xlsx_file() -> Vec<u8> {
        let data = "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";
        let bytes = general_purpose::STANDARD_NO_PAD.decode(data).unwrap().clone();

        bytes
    }

    #[test]
    fn test_csv_parse() {
        let comma_batches = parse_csv(csv_with_comma()).unwrap();
        let semicolon_batches = parse_csv(csv_with_semicolon()).unwrap();

        let expected = "\
            +---+-----+---+\n\
            | a | b   | c |\n\
            +---+-----+---+\n\
            | 1 | 1.1 | x |\n\
            | 2 | 2.2 | y |\n\
            | 3 | 3.3 | z |\n\
            +---+-----+---+\
        ";

        let comma_result: String = pretty::pretty_format_batches(&comma_batches).unwrap().to_string();
        let semicolon_result: String = pretty::pretty_format_batches(&semicolon_batches).unwrap().to_string();

        assert_eq!(comma_result.as_str(), expected);
        assert_eq!(semicolon_result.as_str(), expected);
    }


    #[test]
    fn test_xlsx_parse() {
        let batches = parse_sheet(&xlsx_file()[..]).unwrap();

        let expected = "\
            +---+-----+---+\n\
            | a | b   | c |\n\
            +---+-----+---+\n\
            | 1 | 1.1 | x |\n\
            | 2 | 2.2 | y |\n\
            | 3 | 3.3 | z |\n\
            +---+-----+---+\
        ";

        let result: String = pretty::pretty_format_batches(&batches).unwrap().to_string();

        assert_eq!(result.as_str(), expected);
    }
}
