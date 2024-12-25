use std::error::Error;
use std::fs;
use std::io::Write;
use csv::{ReaderBuilder, WriterBuilder};
use encoding_rs::UTF_8;
use encoding_rs_io::DecodeReaderBytesBuilder;
use std::collections::HashMap;

#[derive(Debug)]
struct ColumnInfo {
    name: String,
    is_numeric: bool,
}

fn main() -> Result<(), Box<dyn Error>> {
    let input_path = "/home/aricept094/mydata/endometriosis/merged_endometriosis_data_cleaned.csv";
    let output_path = "/home/aricept094/mydata/endometriosis/sorted_columns_output.csv";

    // First pass: analyze all rows to determine column types accurately
    let file = fs::File::open(input_path)?;
    let transcoded = DecodeReaderBytesBuilder::new()
        .encoding(Some(UTF_8))
        .bom_sniffing(true)
        .build(file);

    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(transcoded);

    let headers = rdr.headers()?.clone();
    let mut column_numeric_counts: HashMap<String, (usize, usize)> = headers
        .iter()
        .map(|header| (header.to_string(), (0, 0)))
        .collect();

    // Count numeric vs non-numeric values in each column
    for result in rdr.records() {
        let record = result?;
        for (idx, header) in headers.iter().enumerate() {
            let value = record.get(idx).unwrap_or("").trim();
            let (numeric_count, total_count) = column_numeric_counts.get_mut(header).unwrap();
            if !value.is_empty() {
                *total_count += 1;
                if is_numeric_value(value) {
                    *numeric_count += 1;
                }
            }
        }
    }

    // Determine column types based on majority of values (>95% threshold)
    let mut column_info: Vec<ColumnInfo> = headers
        .iter()
        .map(|header| {
            let (numeric_count, total_count) = column_numeric_counts.get(header).unwrap();
            let numeric_ratio = if *total_count > 0 {
                *numeric_count as f64 / *total_count as f64
            } else {
                0.0
            };
            
            ColumnInfo {
                name: header.to_string(),
                is_numeric: numeric_ratio > 0.95  // 95% threshold for numeric classification
            }
        })
        .collect();

    // Sort columns: categorical first, then numeric
    column_info.sort_by(|a, b| {
        if a.is_numeric == b.is_numeric {
            a.name.cmp(&b.name)
        } else {
            a.is_numeric.cmp(&b.is_numeric)
        }
    });

    // Print column classification for verification
    println!("\nColumn Classification:");
    for col in &column_info {
        println!("{}: {}", col.name, if col.is_numeric { "numeric" } else { "categorical" });
    }

    // Create output file and write BOM
    let mut output_file = fs::File::create(output_path)?;
    output_file.write_all(&[0xEF, 0xBB, 0xBF])?;

    // Create CSV writer
    let mut writer = WriterBuilder::new()
        .has_headers(true)
        .from_writer(output_file);

    // Write headers
    let new_headers: Vec<String> = column_info.iter()
        .map(|col| col.name.clone())
        .collect();
    writer.write_record(&new_headers)?;

    // Reset reader for data writing
    let file = fs::File::open(input_path)?;
    let transcoded = DecodeReaderBytesBuilder::new()
        .encoding(Some(UTF_8))
        .bom_sniffing(true)
        .build(file);

    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(transcoded);

    // Write data with reordered columns
    for result in rdr.records() {
        let record = result?;
        let mut new_record: Vec<String> = Vec::new();
        
        for col in &column_info {
            let idx = headers.iter()
                .position(|h| h == &col.name)
                .unwrap();
            new_record.push(record.get(idx).unwrap_or("").to_string());
        }
        
        writer.write_record(&new_record)?;
    }

    writer.flush()?;
    println!("\nCSV processed successfully! Output saved to: {}", output_path);
    Ok(())
}

fn is_numeric_value(value: &str) -> bool {
    if value.trim().is_empty() {
        return false;
    }
    
    // Remove thousand separators and try parsing
    let cleaned_value = value.replace(',', "");
    
    // Try parsing as float
    if cleaned_value.parse::<f64>().is_ok() {
        return true;
    }

    // Try parsing as integer
    if cleaned_value.parse::<i64>().is_ok() {
        return true;
    }

    false
}