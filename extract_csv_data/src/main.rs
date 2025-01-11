use std::fs::{self, File};
use std::io::{self, Read, BufRead, BufReader};
use std::path::{Path, PathBuf};
use csv::{Reader, Writer, ReaderBuilder};

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

fn find_marker_position(file_path: &Path) -> Result<usize, ProcessingError> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    
    for (index, line) in reader.lines().enumerate() {
        if let Ok(line) = line {
            if line.contains(MARKER) {
                println!("Found marker '{}' at line {} with content: {}", MARKER, index + 1, line);
                return Ok(index);
            }
        }
    }
    
    Err(ProcessingError {
        message: format!("Marker '{}' not found in file", MARKER),
    })
}

fn process_csv_file(input_path: &Path, output_dir: &Path) -> Result<(), ProcessingError> {
    println!("\nProcessing file: {}", input_path.display());
    println!("Output directory: {}", output_dir.display());

    // Find marker position
    let marker_pos = find_marker_position(input_path)?;
    println!("Found marker at line: {}", marker_pos + 1);

    // Calculate positions
    let start_row = marker_pos + ROWS_TO_SKIP;
    let end_row = start_row + ROWS_TO_KEEP;
    println!("Selection range: rows {}-{}", start_row + 1, end_row);

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

    // Read and process only the required rows
    let file = File::open(input_path)?;
    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .has_headers(false)
        .from_reader(file);

    let mut current_row = 0;
    let mut rows_written = 0;
    
    for result in reader.records() {
        let record = result?;
        
        // Stop after we've processed all needed rows
        if current_row >= end_row {
            break;
        }
        
        // Process rows in our target range
        if current_row >= start_row && current_row < end_row {
            if record.len() < COLS_TO_KEEP {
                println!("Warning: Row {} has only {} columns (expected {})", 
                    current_row + 1, record.len(), COLS_TO_KEEP);
                continue;
            }
            
            let selected_cols: Vec<String> = record
                .iter()
                .take(COLS_TO_KEEP)
                .map(|s| s.to_string())
                .collect();
            
            // Debug print first and last few rows
            if rows_written < 3 || rows_written >= ROWS_TO_KEEP - 3 {
                println!("Writing row {}: First value = {}, Last value = {}", 
                    current_row + 1,
                    selected_cols.first().unwrap_or(&String::from("N/A")),
                    selected_cols.last().unwrap_or(&String::from("N/A")));
            }
            
            writer.write_record(&selected_cols)?;
            rows_written += 1;
        }
        
        current_row += 1;
    }

    println!("Rows written to output: {}", rows_written);
    
    if rows_written == 0 {
        return Err(ProcessingError {
            message: format!("No rows were written to the output file! Check selection range.")
        });
    }

    if rows_written != ROWS_TO_KEEP {
        println!("Warning: Expected to write {} rows but wrote {}", ROWS_TO_KEEP, rows_written);
    }

    writer.flush()?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let input_dir = PathBuf::from("/home/aricept094/mydata/casia2-4");
    let output_dir = PathBuf::from("/home/aricept094/mydata/casia2-4/conv");
    
    println!("Input directory: {}", input_dir.display());
    println!("Output directory: {}", output_dir.display());
    
    fs::create_dir_all(&output_dir)?;
    println!("Output directory created/verified successfully");

    let mut processed_files = 0;
    let mut failed_files = 0;
    
    for entry in fs::read_dir(input_dir)? {
        let entry = entry?;
        let path = entry.path();
        
        if path.extension().and_then(|s| s.to_str()) == Some("csv") {
            println!("\n=== Processing file: {} ===", path.display());
            match process_csv_file(&path, &output_dir) {
                Ok(_) => {
                    println!("Successfully processed: {}", path.display());
                    processed_files += 1;
                },
                Err(e) => {
                    eprintln!("Error processing {}: {}", path.display(), e.message);
                    failed_files += 1;
                }
            }
        }
    }

    println!("\nProcessing summary:");
    println!("Successfully processed: {} files", processed_files);
    println!("Failed to process: {} files", failed_files);

    Ok(())
}