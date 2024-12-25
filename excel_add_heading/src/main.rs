use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;

fn add_IUIO_to_headings(file_path: &str) -> Result<(), Box<dyn Error>> {
    // Check if the file exists
    if !Path::new(file_path).exists() {
        return Err(format!("File not found: {}", file_path).into());
    }

    // 1. Read the file into memory, modifying the header row
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let mut lines: Vec<String> = Vec::new();
    let mut first_line = true;

    for line in reader.lines() {
        let mut line = line?;
        if first_line {
            // Modify the header row
            let headings: Vec<String> = line.split(',').map(|s| s.trim().to_string()).collect();
            let modified_headings: Vec<String> =
                headings.iter().map(|h| format!("{} IUIO", h)).collect();
            line = modified_headings.join(",");
            first_line = false;
        }
        lines.push(line);
    }

    // 2. Write the modified content back to the file (or a new file)
    let out_file_path = format!("{}_modified.csv", file_path.trim_end_matches(".csv")); // Create a new file name
    let out_file = File::create(out_file_path)?;
    let mut writer = BufWriter::new(out_file);

    for line in lines {
        writeln!(writer, "{}", line)?;
    }

    Ok(())
}

fn main() {
    let file_path = "/home/aricept094/mydata/PCO/IUIO.csv";
    match add_IUIO_to_headings(file_path) {
        Ok(_) => println!("Successfully added 'IUIO' to headings."),
        Err(e) => eprintln!("Error: {}", e),
    }
}