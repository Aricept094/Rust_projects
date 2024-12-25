use std::collections::HashMap;
use std::error::Error;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

fn rename_duplicate_headings(filepath: &str) -> Result<(), Box<dyn Error>> {
    // 1. Read the first line (headings) from the file.
    let path = Path::new(filepath);
    let file = File::open(&path)?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    let header_line = match lines.next() {
        Some(Ok(line)) => line,
        Some(Err(e)) => return Err(Box::new(e)),
        None => return Ok(()), // Empty file, nothing to do
    };

    // 2. Split the header line into individual headings.
    let headings: Vec<String> = header_line.split(',').map(|s| s.trim().to_string()).collect();

    // 3. Check for duplicates and rename them.
    let mut seen_headings: HashMap<String, usize> = HashMap::new();
    let mut new_headings: Vec<String> = Vec::new();
    let mut duplicate_count = 0; // Keep track of the number of duplicates

    for heading in headings {
        let count = seen_headings.entry(heading.clone()).or_insert(0);
        *count += 1;

        if *count == 1 {
            new_headings.push(heading);
        } else {
            new_headings.push(format!("{}_{}", heading, *count));
            duplicate_count += 1; // Increment the duplicate count
        }
    }

    // Print the number of duplicate headings detected for the current file
    println!("Number of duplicate headings detected in {}: {}", filepath, duplicate_count);

    // 4. Write the modified headings back to a temporary file.
    let temp_filepath = format!("{}.tmp", filepath);
    let mut temp_file = File::create(&temp_filepath)?;
    writeln!(temp_file, "{}", new_headings.join(","))?;

    // 5. Write the rest of the lines to the temporary file.
    for line_result in lines {
        let line = line_result?;
        writeln!(temp_file, "{}", line)?;
    }

    // 6. Replace the original file with the temporary file.
    fs::rename(temp_filepath, filepath)?;

    Ok(())
}

fn main() {
    let filepaths = [
        "demographic.csv",
        "IUIO.csv",
        "IVF.csv",
        "neonate freeze.csv",
        "only PCO.csv",
        "paraclinic.csv",
        "Pickup Transfer.csv",
        "pregnancy control.csv",
    ];

    let base_path = "/home/aricept094/mydata/PCO/"; 

    for filepath in filepaths.iter() {
        let full_filepath = format!("{}{}", base_path, filepath); // Construct the full file path

        if let Err(err) = rename_duplicate_headings(&full_filepath) {
            eprintln!("Error processing file {}: {}", full_filepath, err);
        } else {
            println!("Successfully processed file: {}", full_filepath);
        }
    }
}