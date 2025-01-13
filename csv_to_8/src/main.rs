use std::fs;
use std::path::{Path, PathBuf};
use std::error::Error;
use csv::{Reader, Writer};
use std::collections::HashMap;
use rayon::prelude::*;

fn main() -> Result<(), Box<dyn Error>> {
    // Define the Radial_Index values we want to separate
    let radial_indices = vec![1, 4, 8, 12, 16, 24, 28, 32];

    // Input directory
    let input_dir = Path::new("/home/aricept094/mydata/casia_more_than_4/combined_data/");

    // Base output directory
    let base_output_dir = Path::new("/home/aricept094/mydata/casia_more_than_4/combined_data");

    // Create output directories for each Radial_Index
    for &index in &radial_indices {
        let dir_path = base_output_dir.join(format!("radial_{}", index));
        fs::create_dir_all(&dir_path)?;
    }

    // Process each CSV file in the input directory in parallel
    let entries = fs::read_dir(input_dir)?
        .collect::<Result<Vec<_>, _>>()?;

    entries.par_iter().for_each(|entry| {
        let path = entry.path();

        if path.extension().and_then(|s| s.to_str()) == Some("csv") {
            if let Err(e) = process_file(&path, &radial_indices, base_output_dir) {
                eprintln!("Error processing file {:?}: {}", path.file_name().unwrap(), e);
            }
        }
    });

    Ok(())
}

fn process_file(
    input_path: &PathBuf,
    radial_indices: &[i32],
    base_output_dir: &Path,
) -> Result<(), Box<dyn Error>> {
    println!("Processing file: {:?}", input_path.file_name().unwrap());

    // Create a reader
    let mut reader = Reader::from_path(input_path)?;

    // Get headers
    let headers = reader.headers()?.clone();

    // Find Radial_Index column
    let radial_index_col = headers
        .iter()
        .position(|header| header == "Radial_Index")
        .ok_or("Radial_Index column not found")?;

    // Create a HashMap to store writers for each Radial_Index
    let mut writers: HashMap<i32, Writer<std::fs::File>> = HashMap::new();

    // Get the original filename without extension
    let file_stem = input_path
        .file_stem()
        .ok_or("Invalid filename")?
        .to_str()
        .ok_or("Invalid UTF-8 in filename")?;

    // Initialize writers for each Radial_Index
    for &index in radial_indices {
        let output_dir = base_output_dir.join(format!("radial_{}", index));
        let output_path = output_dir.join(format!("{}.csv", file_stem));
        let writer = Writer::from_path(output_path)?;
        writers.insert(index, writer);
    }

    // Write headers to all files
    for writer in writers.values_mut() {
        writer.write_record(&headers)?;
    }

    // Process records
    for result in reader.records() {
        let record = result?;
        if let Some(value) = record.get(radial_index_col) {
            if let Ok(index) = value.parse::<i32>() {
                if let Some(writer) = writers.get_mut(&index) {
                    writer.write_record(&record)?;
                }
            }
        }
    }

    println!("Finished processing: {:?}", input_path.file_name().unwrap());
    Ok(())
}