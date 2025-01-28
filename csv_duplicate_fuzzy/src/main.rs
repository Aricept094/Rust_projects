use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use csv::Writer;
use std::collections::HashMap;

#[derive(Debug)]
struct FileInfo {
    filename: String,
    sequence: u32,
}

#[derive(Debug)]
struct DuplicateReport {
    keep_file: String,
    remove_file: String,
    reason: String,
}

// Helper function to parse file components
fn parse_filename(filename: &str) -> Option<(String, String, u32)> {
    let parts: Vec<&str> = filename.split('_').collect();
    if parts.len() >= 5 {
        // Extract the base part (everything before L/R), the eye indicator (L/R), and the sequence number
        let eye_indicator = parts[4].to_string(); // This should be L or R
        let sequence_str = parts[5].to_string(); // This should be the sequence number (e.g., 002)
        let base_name = parts[..4].join("_"); // Join everything before the eye indicator
        
        // Parse sequence number, removing any non-numeric characters
        if let Ok(sequence) = sequence_str.chars()
            .filter(|c| c.is_digit(10))
            .collect::<String>()
            .parse::<u32>() {
            Some((base_name, eye_indicator, sequence))
        } else {
            None
        }
    } else {
        None
    }
}

fn find_duplicates(dir_path: &Path) -> Result<Vec<DuplicateReport>, Box<dyn Error>> {
    let mut csv_files: Vec<String> = Vec::new();
    for entry in fs::read_dir(dir_path)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("csv") {
            if let Some(file_name) = path.file_name().and_then(|s| s.to_str()) {
                csv_files.push(file_name.to_string());
            }
        }
    }

    // Group files by base name and eye indicator
    let mut file_groups: HashMap<(String, String), Vec<FileInfo>> = HashMap::new();
    
    for filename in csv_files {
        if let Some((base, eye, sequence)) = parse_filename(&filename) {
            let key = (base, eye);
            file_groups.entry(key).or_default().push(FileInfo {
                filename: filename.clone(),
                sequence,
            });
        }
    }

    let mut duplicate_reports = Vec::new();
    
    // Process each group to identify files to keep and remove
    for ((_base, eye), mut files) in file_groups {
        if files.len() > 1 {
            // Sort by sequence number
            files.sort_by_key(|f| f.sequence);
            
            // Keep the lowest sequence number, mark others for removal
            let keep_file = &files[0];
            for remove_file in files.iter().skip(1) {
                duplicate_reports.push(DuplicateReport {
                    keep_file: keep_file.filename.clone(),
                    remove_file: remove_file.filename.clone(),
                    reason: format!(
                        "Keep sequence {} (lower) vs {} (higher) for eye {}",
                        keep_file.sequence,
                        remove_file.sequence,
                        eye
                    ),
                });
            }
        }
    }
    
    Ok(duplicate_reports)
}

fn write_csv_report(reports: &[DuplicateReport], output_path: &Path) -> Result<(), Box<dyn Error>> {
    let mut wtr = Writer::from_path(output_path)?;
    // Write CSV header
    wtr.write_record(&["Keep File", "Remove File", "Reason"])?;
    // Write report data
    for report in reports {
        wtr.write_record(&[
            &report.keep_file,
            &report.remove_file,
            &report.reason,
        ])?;
    }
    wtr.flush()?;
    Ok(())
}

// Function to actually remove the files
fn remove_duplicate_files(dir_path: &Path, reports: &[DuplicateReport]) -> Result<(), Box<dyn Error>> {
    for report in reports {
        let file_path = dir_path.join(&report.remove_file);
        if file_path.exists() {
            println!("Removing file: {}", report.remove_file);
            fs::remove_file(file_path)?;
        }
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let input_dir = Path::new("/home/aricept094/mydata/casia2-4/combined_data");
    let output_dir = Path::new("/home/aricept094/mydata/ANOVA");
    
    if !input_dir.exists() || !input_dir.is_dir() {
        eprintln!("Error: Input directory '{}' does not exist or is not a directory.", input_dir.display());
        return Ok(());
    }
    
    fs::create_dir_all(output_dir)?;
    let output_file_path = output_dir.join("duplicate_removal_report_casia2-4.csv");
    
    println!("Scanning for duplicate CSV files in: {}", input_dir.display());
    let duplicate_reports = find_duplicates(input_dir)?;
    
    if duplicate_reports.is_empty() {
        println!("No duplicate CSV files found.");
    } else {
        println!("Found duplicate CSV files. Writing report to: {}", output_file_path.display());
        write_csv_report(&duplicate_reports, &output_file_path)?;
        
        println!("\nDuplicate Files Report:");
        println!("------------------------------------------------------------------");
        println!("{: <50} | {: <50} | {: <30}", "Keep File", "Remove File", "Reason");
        println!("------------------------------------------------------------------");
        for report in &duplicate_reports {
            println!("{: <50} | {: <50} | {: <30}", 
                report.keep_file, 
                report.remove_file, 
                report.reason
            );
        }
        println!("------------------------------------------------------------------");
        
        println!("\nRemoving duplicate files...");
        remove_duplicate_files(input_dir, &duplicate_reports)?;
        println!("Duplicate files have been removed.");
    }
    
    println!("Process completed.");
    Ok(())
}