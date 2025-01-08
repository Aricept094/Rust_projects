use calamine::{open_workbook, Reader, Xlsx};
use std::fs::{self, create_dir_all};
use std::path::Path;
use csv::Writer;
use anyhow::{Result, Context};

fn main() -> Result<()> {
    // Define input and output paths
    let input_path = "/home/aricept094/mydata/Book2.xlsx";
    let output_dir = "/home/aricept094/mydata/sheets";

    // Create output directory if it doesn't exist
    create_dir_all(output_dir)?;

    // Open the workbook
    let mut workbook: Xlsx<_> = open_workbook(input_path)
        .with_context(|| format!("Failed to open workbook at {}", input_path))?;

    // Get all sheet names
    let sheet_names = workbook.sheet_names().to_vec();

    // Process each sheet
    for sheet_name in sheet_names {
        process_sheet(&mut workbook, &sheet_name, output_dir)?;
    }

    println!("All sheets have been successfully converted to CSV!");
    Ok(())
}

fn process_sheet(workbook: &mut Xlsx<impl std::io::Read + std::io::Seek>, 
                sheet_name: &str, 
                output_dir: &str) -> Result<()> {
    // Get the sheet
    let range = workbook.worksheet_range(sheet_name)
        .with_context(|| format!("Failed to read sheet {}", sheet_name))?;

    // Create CSV writer
    let output_path = Path::new(output_dir).join(format!("{}.csv", sheet_name));
    let mut writer = Writer::from_path(&output_path)
        .with_context(|| format!("Failed to create CSV writer for {}", output_path.display()))?;

    // Process each row
    for row in range.rows() {
        // Convert each cell to string
        let row_data: Vec<String> = row.iter()
            .map(|cell| cell.to_string())
            .collect();

        // Write row to CSV
        writer.write_record(&row_data)
            .with_context(|| "Failed to write row to CSV")?;
    }

    writer.flush()
        .with_context(|| format!("Failed to flush CSV writer for {}", output_path.display()))?;

    println!("Processed sheet: {}", sheet_name);
    Ok(())
}