use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::{BufReader, Read, Write};
use csv::{Reader, Writer};
use encoding_rs::UTF_8;
use encoding_rs_io::DecodeReaderBytesBuilder;
use itertools::Itertools;

#[derive(Debug)]
struct Column {
    header: String,
    original_index: usize,
    values: Vec<String>,
}

fn calculate_similarity(vec1: &[String], vec2: &[String]) -> f64 {
    let len = vec1.len().min(vec2.len());
    if len == 0 {
        return 0.0;
    }

    let matching = vec1.iter()
        .zip(vec2.iter())
        .filter(|(a, b)| a == b)
        .count();

    (matching as f64 / len as f64) * 100.0
}

fn main() -> Result<(), Box<dyn Error>> {
    // Open the file with UTF-8 BOM detection
    let file = File::open("/home/aricept094/mydata/PCO/sorted_columns_cleaned_output_good_targets.csv")?;
    let decoder = DecodeReaderBytesBuilder::new()
        .encoding(Some(UTF_8))
        .bom_sniffing(true)
        .build(file);
    let reader = BufReader::new(decoder);

    // Create CSV reader with flexible configuration
    let mut csv_reader = csv::ReaderBuilder::new()
        .flexible(true)
        .from_reader(reader);
    
    // Read headers and preserve original indices
    let headers = csv_reader.headers()?.clone();
    let mut columns: Vec<Column> = headers
        .iter()
        .enumerate()
        .map(|(idx, header)| Column {
            header: header.to_string(),
            original_index: idx,
            values: Vec::new(),
        })
        .collect();

    // Read data into columns
    for result in csv_reader.records() {
        let record = result?;
        for (idx, value) in record.iter().enumerate() {
            if idx < columns.len() {
                columns[idx].values.push(value.to_string());
            }
        }
    }

    // Calculate similarities
    let mut similarities = Vec::new();
    for i in 0..columns.len() {
        for j in (i + 1)..columns.len() {
            let similarity = calculate_similarity(&columns[i].values, &columns[j].values);
            similarities.push((
                columns[i].header.clone(),
                columns[j].header.clone(),
                similarity,
                columns[i].original_index,
                columns[j].original_index
            ));
        }
    }

    // Sort by similarity percentage (descending) and original indices
    similarities.sort_by(|a, b| {
        b.2.partial_cmp(&a.2)
            .unwrap()
            .then(a.3.cmp(&b.3))
            .then(a.4.cmp(&b.4))
    });

    // Write results to CSV with UTF-8 BOM
    let mut writer = Writer::from_path("column_similarities.csv")?;
    
    // Write UTF-8 BOM
    let mut file = File::create("column_similarities.csv")?;
    file.write_all(&[0xEF, 0xBB, 0xBF])?;
    
    let mut writer = Writer::from_writer(file);
    writer.write_record(&["Column 1", "Column 2", "Similarity %", "Column 1 Index", "Column 2 Index"])?;

    for (col1, col2, similarity, idx1, idx2) in similarities {
        writer.write_record(&[
            &col1,
            &col2,
            &format!("{:.2}", similarity),
            &idx1.to_string(),
            &idx2.to_string(),
        ])?;
    }

    writer.flush()?;
    println!("Analysis complete. Results saved to column_similarities.csv");

    Ok(())
}