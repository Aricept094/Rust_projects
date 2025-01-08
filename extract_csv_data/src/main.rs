use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use csv::{Reader, Writer};

const MARKER: &str = "[Axial Keratometric]";
const ROWS_TO_SKIP: usize = 3;
const ROWS_TO_KEEP: usize = 256;
const COLS_TO_KEEP: usize = 32;

#[derive(Debug)]
struct ProcessingError {
    message: String,
}

impl From<io::Error> for ProcessingError {
    fn from(error: io::Error) -> Self {
        ProcessingError {
            message: error.to_string(),
        }
    }
}

impl From<csv::Error> for ProcessingError {
    fn from(error: csv::Error) -> Self {
        ProcessingError {
            message: error.to_string(),
        }
    }
}

fn find_marker_position(reader: &mut Reader<File>) -> Option<usize> {
    for (index, result) in reader.records().enumerate() {
        if let Ok(record) = result {
            if let Some(first_cell) = record.get(0) {
                if first_cell.contains(MARKER) {
                    return Some(index);
                }
            }
        }
    }
    None
}

fn process_csv_file(input_path: &Path, output_dir: &Path) -> Result<(), ProcessingError> {
    println!("Processing file: {}", input_path.display());
    println!("Output directory: {}", output_dir.display());

    // Create reader
    let file = File::open(input_path)?;
    let mut reader = csv::Reader::from_reader(file);

    // Find marker position
    let marker_pos = find_marker_position(&mut reader)
        .ok_or_else(|| ProcessingError {
            message: format!("Marker '{}' not found in file", MARKER),
        })?;

    // Print selection range for debugging
    println!("Found marker at row: {}", marker_pos + 1);
    println!("Selection starts at row: {}", marker_pos + ROWS_TO_SKIP + 1);
    println!("Selection ends at row: {}", marker_pos + ROWS_TO_SKIP + ROWS_TO_KEEP);
    println!("Selecting columns: 1 to {}", COLS_TO_KEEP);

    // Reopen the file to read from the beginning
    let file = File::open(input_path)?;
    let mut reader = csv::Reader::from_reader(file);

    // Prepare output file
    let output_path = output_dir.join(
        input_path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned(),
    );
    let output_file = File::create(&output_path)?;
    let mut writer = Writer::from_writer(output_file);

    // Skip rows until the target section
    let start_row = marker_pos + ROWS_TO_SKIP;
    let mut records = reader.records();
    for _ in 0..start_row {
        records.next();
    }

    // Process the target section
    let mut rows_written = 0;
    for _ in 0..ROWS_TO_KEEP {
        if let Some(Ok(record)) = records.next() {
            let selected_cols: Vec<String> = record
                .iter()
                .take(COLS_TO_KEEP)
                .map(|s| s.to_string())
                .collect();
            writer.write_record(&selected_cols)?;
            rows_written += 1;
        }
    }

    println!("Wrote {} rows to {}", rows_written, output_path.display());
    writer.flush()?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let input_dir = PathBuf::from("/home/aricept094/mydata/sheets");
    let output_dir = PathBuf::from("/home/aricept094/mydata/sheets/conv");
    
    println!("Looking for CSV files in: {}", input_dir.display());
    println!("Creating output directory: {}", output_dir.display());
    
    match fs::create_dir_all(&output_dir) {
        Ok(_) => println!("Output directory created/verified successfully"),
        Err(e) => println!("Error creating output directory: {}", e),
    };

    let mut processed_files = 0;
    
    // Process all CSV files in the input directory
    for entry in fs::read_dir(input_dir)? {
        let entry = entry?;
        let path = entry.path();
        
        if path.extension().and_then(|s| s.to_str()) == Some("csv") {
            println!("\nFound CSV file: {}", path.display());
            match process_csv_file(&path, &output_dir) {
                Ok(_) => {
                    println!("Successfully processed: {}", path.display());
                    processed_files += 1;
                },
                Err(e) => eprintln!("Error processing {}: {}", path.display(), e.message),
            }
        }
    }

    if processed_files == 0 {
        println!("No CSV files were processed!");
    } else {
        println!("\nSuccessfully processed {} CSV files", processed_files);
    }

    Ok(())
}