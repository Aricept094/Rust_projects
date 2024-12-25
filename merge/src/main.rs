use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Write;
use std::path::Path;
use csv::{ReaderBuilder, WriterBuilder};
use encoding_rs::UTF_8;
use encoding_rs_io::DecodeReaderBytesBuilder;
use thiserror::Error;
use indicatif::{ProgressBar, ProgressStyle};

#[derive(Debug, Error)]
enum DataError {
    #[error("File I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("CSV error: {0}")]
    Csv(#[from] csv::Error),
    #[error("Column not found: {0} in file: {1}")]
    ColumnNotFound(String, String),
}


// Function to create csv reader
fn create_reader(file_path: &str) -> Result<csv::Reader<impl std::io::Read>, DataError> {
    let file = File::open(file_path)?;
    let decoder = DecodeReaderBytesBuilder::new()
        .encoding(Some(UTF_8))
        .bom_sniffing(true)
        .build(file);
    
    let reader = ReaderBuilder::new()
        .flexible(true)
        .has_headers(true)
        .from_reader(decoder);
    
    Ok(reader)
}

// Function to read national IDs from PCO file
fn read_pco_national_ids(file_path: &str, id_column_name: &str) -> Result<HashSet<String>, DataError> {
    let mut reader = create_reader(file_path)?;

    let headers = reader.headers()?;
    let id_column_index = headers.iter()
        .position(|h| h == id_column_name)
        .ok_or_else(|| DataError::ColumnNotFound(id_column_name.to_string(), file_path.to_string()))?;

    let mut national_ids = HashSet::new();
    for result in reader.records() {
        let record = result?;
        if let Some(id) = record.get(id_column_index) {
            national_ids.insert(id.to_string());
        }
    }
    println!("Found {} national IDs in PCO file", national_ids.len());
    Ok(national_ids)
}

// Helper function to extract data for a record
fn extract_record_data(
    record: &csv::StringRecord,
    file_name: &str,
    file_headers: &[String],
    id_column_index: usize,
    national_id: &str
) -> HashMap<String, String> {
    let mut row_data = HashMap::new();
    // Generate national ID header with file prefix
    let id_header = format!("{}_کد ملی", file_name);
    row_data.insert(id_header, national_id.to_string());

    for (i, value) in record.iter().enumerate() {
        if i != id_column_index {
             let header_name = format!("{}_{}", file_name, &file_headers[i]);
            row_data.insert(header_name, value.to_string());
        }
    }
    row_data
}

// Function to process a single file and extract matching records
fn process_file(
    file_path: &str,
    file_name: &str,
    national_ids: &HashSet<String>,
    data_map: &mut HashMap<String, HashMap<String, String>>,
    id_headers: &mut Vec<String>,
    name_headers: &mut HashMap<String, Vec<String>>,
    other_headers: &mut HashMap<String, Vec<String>>,
) -> Result<(), DataError> {
    println!("Processing {}", file_name);

    let pb = ProgressBar::new_spinner();
    pb.set_style(ProgressStyle::default_spinner()
        .template("{spinner:.green} [{elapsed_precise}] {msg}")
        .unwrap());

    let mut reader = create_reader(file_path)?;

    // Get headers
    let headers = reader.headers()?;
    let file_headers: Vec<String> = headers.iter().map(String::from).collect();

    // Find the index of the national ID column
    let id_column_index = headers.iter()
        .position(|h| h == "کد ملی")
        .ok_or_else(|| DataError::ColumnNotFound("کد ملی".to_string(), file_name.to_string()))?;

    // Add national ID header to the id_headers list
    let id_header = format!("{}_کد ملی", file_name);
    id_headers.push(id_header);

    // Add headers to appropriate maps (excluding the ID column)
    for (i, header) in file_headers.iter().enumerate() {
        if i != id_column_index {
            let full_header = format!("{}_{}", file_name, header);
            if header.contains("نام") {
                name_headers.entry(String::from(header)).or_default().push(full_header);
            } else {
                other_headers.entry(String::from(header)).or_default().push(full_header);
            }
        }
    }

    // Read records
    let mut records_processed = 0;
    for result in reader.records() {
        let record = result?;
        if let Some(id) = record.get(id_column_index) {
            if national_ids.contains(id) {
                let row_data = data_map.entry(id.to_string()).or_default();

                let extracted_data = extract_record_data(&record, file_name, &file_headers, id_column_index, id);
                row_data.extend(extracted_data);
                records_processed += 1;
            }
        }
    }
    println!("Processed {} matching records from {}", records_processed, file_name);
    Ok(())
}

fn main() -> Result<(), DataError> {
    let base_path = Path::new("/home/aricept094/mydata/endometriosis");

    // List of all files to process
    struct Config {
        files: Vec<&'static str>,
        id_column_name: String,
        output_filename: String,
    }

    let config = Config {
        files: vec![
            "demographic.csv",
            "IUIO.csv",
            "IVF.csv",
            "neonate freeze.csv",
            "paraclinic.csv",
            "Pickup Transfer.csv",
            "pregnancy control.csv",
        ],
        id_column_name: "کد ملی".to_string(),
        output_filename: "/home/aricept094/mydata/endometriosis/merged_endometriosis_data.csv".to_string(),
    };

    // First, read national IDs from PCO file
    let pco_path = base_path.join("/home/aricept094/mydata/endometriosis/endometrioma.csv");
    let national_ids = read_pco_national_ids(pco_path.to_str().unwrap(), &config.id_column_name)?;

    let mut data_map: HashMap<String, HashMap<String, String>> = HashMap::new();
    let mut id_headers: Vec<String> = Vec::new();
    let mut name_headers: HashMap<String, Vec<String>> = HashMap::new();
    let mut other_headers: HashMap<String, Vec<String>> = HashMap::new();

    // Process each file
    for file_name in &config.files {
        let file_path = base_path.join(file_name);
        process_file(
            file_path.to_str().unwrap(),
            file_name,
            &national_ids,
            &mut data_map,
            &mut id_headers,
            &mut name_headers,
            &mut other_headers,
        )?;
    }

    println!("Writing merged data...");
    println!("Total ID columns: {}", id_headers.len());
    println!("Total name column groups: {}", name_headers.len());
    println!("Total other column groups: {}", other_headers.len());
    println!("Total records: {}", data_map.len());

    // Create final headers list with IDs first, then names, then others
    let mut final_headers: Vec<String> = Vec::with_capacity(id_headers.len() + name_headers.values().map(|v| v.len()).sum::<usize>() + other_headers.values().map(|v| v.len()).sum::<usize>());
    final_headers.extend(id_headers);
    for headers in name_headers.values() {
      final_headers.extend(headers.clone())
    }
    for headers in other_headers.values() {
        final_headers.extend(headers.clone());
    }

    // Write merged data to a new CSV file with proper UTF-8 encoding
    let output_path = base_path.join(&config.output_filename);
    let mut file = File::create(output_path)?;
    
    // Write UTF-8 BOM
    file.write_all(&[0xEF, 0xBB, 0xBF])?;
    
    let mut wtr = WriterBuilder::new()
        .has_headers(true)
        .from_writer(file);

    // Write headers
    wtr.write_record(&final_headers)?;

    // Write data
    for (_id, row_data) in &data_map {
        let row: Vec<String> = final_headers.iter()
            .map(|header| {
                row_data.get(header).cloned().unwrap_or_default()
            })
            .collect();
        wtr.write_record(&row)?;
    }
    println!("Data has been successfully merged and saved to '{}'", config.output_filename);
    Ok(())
}