use std::fs;
use std::path::{Path, PathBuf};
use std::error::Error;
use csv::{Reader, Writer};
use std::collections::HashSet;

fn main() -> Result<(), Box<dyn Error>> {
    // Define the allowed Radial_Index values
    let allowed_values: HashSet<String> = vec!["1", "4", "8", "12", "16", "24", "28", "32"]
        .into_iter()
        .map(String::from)
        .collect();

    // Set up input and output directories
    let input_dir = Path::new("/home/aricept094/mydata/casia2-4/combined_data");
    let output_dir = Path::new("/home/aricept094/mydata/casia2-4/combined_data/limited");

    // Create output directory if it doesn't exist
    fs::create_dir_all(output_dir)?;

    // Get all CSV files in the input directory
    let entries = fs::read_dir(input_dir)?;

    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        
        if path.extension().and_then(|s| s.to_str()) == Some("csv") {
            process_file(&path, &allowed_values, output_dir)?;
        }
    }

    Ok(())
}

fn process_file(
    input_path: &PathBuf,
    allowed_values: &HashSet<String>,
    output_dir: &Path,
) -> Result<(), Box<dyn Error>> {
    // Create reader for input file
    let mut reader = Reader::from_path(input_path)?;
    
    // Get the filename for the output file
    let filename = input_path.file_name()
        .ok_or("Invalid filename")?
        .to_str()
        .ok_or("Invalid UTF-8 in filename")?;
    
    let output_path = output_dir.join(filename);
    
    // Create writer for output file
    let mut writer = Writer::from_path(&output_path)?;
    
    // Write headers
    let headers = reader.headers()?.clone();
    writer.write_record(&headers)?;
    
    // Find index of Radial_Index column
    let radial_index = headers.iter()
        .position(|header| header == "Radial_Index")
        .ok_or("Radial_Index column not found")?;

    // Process records
    for result in reader.records() {
        let record = result?;
        if let Some(value) = record.get(radial_index) {
            if allowed_values.contains(value) {
                writer.write_record(&record)?;
            }
        }
    }

    println!("Processed: {}", filename);
    Ok(())
}