use rayon::prelude::*;
use std::fs::{self, File};
use std::io::{self, BufReader};
use std::path::{Path, PathBuf};
use csv::{ReaderBuilder, Writer};

// ----------------- Configuration -----------------
// Marker -> number-of-rows-to-skip mapping
static MARKERS_AND_SKIPS: &[(&str, usize)] = &[
    ("[Pachymetry]", 3),
    ("[Axial Posterior]", 3),
    ("[Axial Anterior]", 3),
    ("[Height Anterior]", 3),
    ("[Height Posterior]", 3),
    ("[Axial Keratometric]", 3),
    ("[Elevation Anterior]", 11),
    ("[Elevation Posterior]", 11),
];

const ROWS_TO_KEEP: usize = 256;
const COLS_TO_KEEP: usize = 32;

// Directories to process
static DIRECTORIES: &[&str] = &[
    "/home/aricept094/mydata/casia_more_than_4",
    "/home/aricept094/mydata/casia_less_than_1",
    "/home/aricept094/mydata/casia1-2",
    "/home/aricept094/mydata/casia2-4",
    "/home/aricept094/mydata/sheets",
];

// ----------------- Error Handling -----------------
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

// --------------------------------------------------
fn find_marker_row_index(csv_path: &Path, marker: &str) -> Result<usize, ProcessingError> {
    let file = File::open(csv_path)?;
    let buffered = BufReader::new(file);

    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .has_headers(false)
        .from_reader(buffered);

    for (i, row_result) in reader.records().enumerate() {
        let row = row_result?;
        if let Some(first_col) = row.get(0) {
            if first_col.trim() == marker {
                return Ok(i);
            }
        }
    }

    Err(ProcessingError {
        message: format!("Marker '{}' not found in file: {}", marker, csv_path.display()),
    })
}

// --------------------------------------------------
fn process_csv_for_marker(
    input_path: &Path,
    base_output_dir: &Path,
    marker: &str,
    rows_to_skip: usize,
) -> Result<(), ProcessingError> {
    // 1. Find the row containing the marker
    let marker_row_index = find_marker_row_index(input_path, marker)?;

    // 2. Define the range
    let start_row = marker_row_index + rows_to_skip;
    let end_row = start_row + ROWS_TO_KEEP;

    // 3. Create term-specific directory within the output directory
    let term_dir = base_output_dir.join(marker.trim_matches(&['[', ']'][..]));
    fs::create_dir_all(&term_dir)?;

    // Build a file name that includes the term name at the beginning
    let marker_label = marker.trim_matches(&['[', ']'][..]).replace(' ', "_");
    let original_filename = input_path.file_name().unwrap().to_string_lossy();
    let out_filename = format!("{}_{}", marker_label, original_filename);
    let out_path = term_dir.join(out_filename);

    let out_file = File::create(&out_path)?;
    let mut writer = Writer::from_writer(out_file);

    // 4. Read CSV again to copy just the target rows
    let file = File::open(input_path)?;
    let buffered = BufReader::new(file);
    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .has_headers(false)
        .from_reader(buffered);

    let mut rows_written = 0;

    for (i, row_result) in reader.records().enumerate() {
        if i >= end_row {
            break;
        }
        if i >= start_row && i < end_row {
            let row = row_result?;
            if row.len() < COLS_TO_KEEP {
                eprintln!(
                    "Warning: row {} in '{}' has only {} columns (expected {}). Skipping row.",
                    i + 1,
                    input_path.display(),
                    row.len(),
                    COLS_TO_KEEP
                );
                continue;
            }
            let truncated: Vec<String> = row
                .iter()
                .take(COLS_TO_KEEP)
                .map(|s| s.to_string())
                .collect();

            writer.write_record(&truncated)?;
            rows_written += 1;
        }
    }

    writer.flush()?;

    if rows_written == 0 {
        return Err(ProcessingError {
            message: format!(
                "No rows written for marker '{}' in file '{}'. (start={}, end={})",
                marker,
                input_path.display(),
                start_row,
                end_row
            ),
        });
    }

    if rows_written != ROWS_TO_KEEP {
        eprintln!(
            "Warning: For marker '{}', expected to write {} rows, but wrote {}.",
            marker, ROWS_TO_KEEP, rows_written
        );
    }

    println!(
        "Created '{}', rows written: {}, marker='{}'",
        out_path.display(),
        rows_written,
        marker
    );
    Ok(())
}

// --------------------------------------------------
fn process_csv_for_all_markers(input_path: &Path, output_dir: &Path) {
    for (marker, skip) in MARKERS_AND_SKIPS {
        match process_csv_for_marker(input_path, output_dir, marker, *skip) {
            Ok(_) => { /* success */ }
            Err(e) => {
                eprintln!(
                    "Skipping marker '{}' in file '{}': {}",
                    marker,
                    input_path.display(),
                    e.message
                );
            }
        }
    }
}

// --------------------------------------------------
fn process_directory(dir_str: &str) -> Result<(usize, usize), Box<dyn std::error::Error>> {
    let input_dir = PathBuf::from(dir_str);
    let output_dir = input_dir.join("processed_data");
    fs::create_dir_all(&output_dir)?;

    // Create directories for each term
    for (marker, _) in MARKERS_AND_SKIPS {
        let term_dir = output_dir.join(marker.trim_matches(&['[', ']'][..]));
        fs::create_dir_all(&term_dir)?;
    }

    let entries = fs::read_dir(&input_dir)?
        .filter_map(|res| res.ok())
        .map(|entry| entry.path())
        .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("csv"))
        .collect::<Vec<_>>();

    use std::sync::atomic::{AtomicUsize, Ordering};
    let processed_count = AtomicUsize::new(0);
    let failed_count = AtomicUsize::new(0);

    entries.par_iter().for_each(|path| {
        let result = std::panic::catch_unwind(|| {
            process_csv_for_all_markers(path, &output_dir);
        });
        match result {
            Ok(_) => {
                processed_count.fetch_add(1, Ordering::SeqCst);
            }
            Err(_) => {
                eprintln!("Panic processing file {}. Skipping.", path.display());
                failed_count.fetch_add(1, Ordering::SeqCst);
            }
        }
    });

    Ok((
        processed_count.load(Ordering::SeqCst),
        failed_count.load(Ordering::SeqCst),
    ))
}

// --------------------------------------------------
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut total_processed_files = 0;
    let mut total_failed_files = 0;

    for dir_str in DIRECTORIES {
        println!("\n===== Processing directory: {} =====", dir_str);
        match process_directory(dir_str) {
            Ok((processed, failed)) => {
                println!(
                    "Finished directory {}: processed {} files, failed {} files.",
                    dir_str, processed, failed
                );
                total_processed_files += processed;
                total_failed_files += failed;
            }
            Err(e) => {
                eprintln!("Cannot process directory {}: {}", dir_str, e);
            }
        }
    }

    println!(
        "\n========== Summary ==========\n\
         Total processed files: {}\n\
         Total failed files: {}\n",
        total_processed_files,
        total_failed_files
    );

    Ok(())
}