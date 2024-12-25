use std::time::Instant;
use std::path::Path;
use csv::{ReaderBuilder, WriterBuilder};
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use encoding_rs_io::DecodeReaderBytesBuilder;

fn number_to_excel_column(mut n: usize) -> String {
    let mut result = String::new();
    n += 1;

    while n > 0 {
        n -= 1;
        let remainder = n % 26;
        result.insert(0, (b'A' + remainder as u8) as char);
        n /= 26;
    }

    result
}

fn process_csv(input_path: &str, output_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let timer = Instant::now();
    println!("Processing file: {}", input_path);

    // Open input file with proper UTF-8 decoding
    let file = File::open(input_path)?;
    let transcoded = DecodeReaderBytesBuilder::new()
        .encoding(Some(encoding_rs::UTF_8))
        .bom_sniffing(true)
        .build(file);
    
    let buf_reader = BufReader::new(transcoded);
    let mut rdr = ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(buf_reader);

    let mut data: Vec<Vec<String>> = Vec::new();
    for result in rdr.records() {
        let record = result?;
        data.push(record.iter().map(|s| s.to_string()).collect());
    }

    if data.is_empty() {
        return Err("CSV file is empty".into());
    }

    let height = data.len();
    let width = data[0].len();

    // Calculate empty percentages for columns
    let mut column_empty_percentages: Vec<(usize, f64)> = vec![];

    for col_idx in 0..width {
        let mut empty_count = 0;

        for row in &data {
            if col_idx < row.len() {
                if row[col_idx].trim().is_empty() {
                    empty_count += 1;
                }
            } else {
                empty_count += 1;
            }
        }

        let percentage = (empty_count as f64 / height as f64) * 100.0;
        column_empty_percentages.push((col_idx, percentage));
    }

    // Calculate empty percentages for rows
    let mut row_empty_percentages: Vec<(usize, f64)> = vec![];

    for (row_idx, row) in data.iter().enumerate() {
        let mut empty_count = 0;

        for col_idx in 0..width {
            if col_idx < row.len() {
                if row[col_idx].trim().is_empty() {
                    empty_count += 1;
                }
            } else {
                empty_count += 1;
            }
        }

        let percentage = (empty_count as f64 / width as f64) * 100.0;
        row_empty_percentages.push((row_idx, percentage));
    }

    // Identify columns to keep (less than 70.0% empty)
    let columns_to_keep: Vec<usize> = column_empty_percentages.iter()
        .filter(|(_, percentage)| *percentage < 70.0)
        .map(|(idx, _)| *idx)
        .collect();

    // Identify rows to keep (less than 90.77% empty)
    let rows_to_keep: Vec<usize> = row_empty_percentages.iter()
        .filter(|(_, percentage)| *percentage < 99.77)
        .map(|(idx, _)| *idx)
        .collect();

    // Create output file and write BOM
    let mut output_file = BufWriter::new(File::create(output_path)?);
    output_file.write_all(&[0xEF, 0xBB, 0xBF])?; // Write UTF-8 BOM

    // Create CSV writer
    let mut writer = WriterBuilder::new()
        .flexible(true)
        .from_writer(output_file);

    // Write data for kept columns and rows
    for &original_row_idx in &rows_to_keep {
        if let Some(row) = data.get(original_row_idx) {
            let filtered_row: Vec<String> = columns_to_keep.iter()
                .map(|&original_col_idx| {
                    if original_col_idx < row.len() {
                        row[original_col_idx].clone()
                    } else {
                        String::new()
                    }
                })
                .collect();
            
            writer.write_record(&filtered_row)?;
        }
    }

    // Flush the writer to ensure all data is written
    writer.flush()?;

    println!("\nColumn analysis:");
    println!("Original columns: {}", width);
    println!("Columns kept: {}", columns_to_keep.len());
    println!("Columns dropped: {}", width - columns_to_keep.len());
    println!("Dropped columns (≥70% empty):");

    for (idx, percentage) in column_empty_percentages.iter() {
        if *percentage >= 70.0 {
            println!("Column {} ({}): {:.2}% empty",
                     number_to_excel_column(*idx),
                     idx + 1,
                     percentage);
        }
    }

    println!("\nRow analysis:");
    println!("Original rows: {}", height);
    println!("Rows kept: {}", rows_to_keep.len());
    println!("Rows dropped: {}", height - rows_to_keep.len());
    println!("Dropped rows (≥90.77% empty):");

    for (idx, percentage) in row_empty_percentages.iter() {
        if *percentage >= 90.77 {
            println!("Row {}: {:.2}% empty",
                     idx + 1,
                     percentage);
        }
    }

    println!("\nProcessing completed in {:?}", timer.elapsed());
    println!("Output saved to: {}", output_path);

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let files = vec![
        ("/home/aricept094/mydata/endometriosis/merged_endometriosis_data.csv", "/home/aricept094/mydata/endometriosis/merged_endometriosis_data_cleaned.csv"),
    ];

    for (input_file, output_name) in files {
        // Create output path in same directory as input file
        let input_path = Path::new(input_file);
        let parent_dir = input_path.parent().unwrap_or_else(|| Path::new(""));
        let output_path = parent_dir.join(output_name);

        match process_csv(input_file, output_path.to_str().unwrap()) {
            Ok(_) => println!("\nSuccessfully processed {}", input_file),
            Err(e) => println!("\nError processing {}: {}", input_file, e),
        }
    }

    Ok(())
}