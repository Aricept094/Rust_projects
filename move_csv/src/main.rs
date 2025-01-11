use std::fs;
use std::path::Path;
use std::io;
use walkdir::WalkDir;

fn main() -> io::Result<()> {
    // Use the exact directories specified with corrected path
    let source_dir = r#"/mnt/c/Work/casia - Copy/raw data/casia2-4"#;
    let dest_dir = "/home/aricept094/mydata/casia2-4";  // Corrected username

    println!("Checking source directory...");
    match fs::read_dir(&source_dir) {
        Ok(entries) => {
            let csv_count = entries
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.path().extension()
                        .map_or(false, |ext| ext.to_string_lossy().to_lowercase() == "csv")
                })
                .count();
            println!("Found {} CSV files in source directory", csv_count);
        },
        Err(e) => {
            eprintln!("Error accessing source directory: {}", e);
            return Err(e);
        }
    }

    // Create destination directory if it doesn't exist
    println!("Creating destination directory if it doesn't exist...");
    match fs::create_dir_all(&dest_dir) {
        Ok(_) => println!("Destination directory ready: {}", dest_dir),
        Err(e) => {
            eprintln!("Error creating destination directory: {}", e);
            return Err(e);
        }
    }

    // Counter for copied files
    let mut copied_files = 0;
    let mut failed_files = 0;

    println!("\nStarting file copy process...");
    println!("Source directory: {}", source_dir);
    println!("Destination directory: {}", dest_dir);

    // Walk through the source directory recursively
    for entry in WalkDir::new(&source_dir)
        .follow_links(true)
        .into_iter()
        .filter_map(|e| match e {
            Ok(entry) => Some(entry),
            Err(err) => {
                eprintln!("Error accessing entry: {}", err);
                None
            }
        }) {
            
        let path = entry.path();
        
        // Check if the file is a CSV
        if path.is_file() && path.extension().map_or(false, |ext| ext.to_string_lossy().to_lowercase() == "csv") {
            // Get the file name
            let file_name = path.file_name().unwrap();
            
            // Create destination path
            let dest_path = Path::new(&dest_dir).join(file_name);
            
            // Copy the file
            println!("\nCopying: {} -> {}", path.display(), dest_path.display());
            
            match fs::copy(path, &dest_path) {
                Ok(bytes) => {
                    println!("✓ Successfully copied: {} ({} bytes)", file_name.to_string_lossy(), bytes);
                    copied_files += 1;
                },
                Err(e) => {
                    eprintln!("✗ Failed to copy {}: {}", file_name.to_string_lossy(), e);
                    failed_files += 1;
                }
            }
        }
    }

    // Print summary
    println!("\nCopy operation completed:");
    println!("✓ Successfully copied files: {}", copied_files);
    println!("✗ Failed copies: {}", failed_files);
    println!("Total files processed: {}", copied_files + failed_files);

    // Verify destination
    match fs::read_dir(&dest_dir) {
        Ok(entries) => {
            let copied_count = entries
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.path().extension()
                        .map_or(false, |ext| ext.to_string_lossy().to_lowercase() == "csv")
                })
                .count();
            println!("\nVerification: Found {} CSV files in destination directory", copied_count);
        },
        Err(e) => eprintln!("\nError verifying destination directory: {}", e)
    }

    Ok(())
}