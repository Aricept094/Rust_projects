use calamine::{Reader, open_workbook, Xlsx, DataType};
use std::collections::HashMap;
use std::time::Instant;
use std::fs::File;
use std::io::Write;

#[derive(Debug)]
struct EmptyAnalysis {
    empty_percentages: Vec<(String, f64)>,
    total_cells: usize,
}

fn save_to_csv(analysis: &EmptyAnalysis, filename: &str) -> std::io::Result<()> {
    let mut file = File::create(filename)?;

    // Write CSV header
    writeln!(file, "Name,Empty Percentage")?;

    // Write data
    for (name, percentage) in &analysis.empty_percentages {
        writeln!(file, "{},{:.2}", name, percentage)?;
    }

    Ok(())
}

fn column_name(col_idx: usize) -> String {
    let mut col_str = String::new();
    let mut n = col_idx as i32;
    while n >= 0 {
        let rem = n % 26;
        col_str.insert(0, (65 + rem) as u8 as char);
        n = (n / 26) -1;
        if n < 0{
            break;
        }
    }
    col_str
}

fn analyze_excel(filepath: &str) -> Result<(EmptyAnalysis, EmptyAnalysis), Box<dyn std::error::Error>> {
    let timer = Instant::now();
    println!("Analyzing file: {}", filepath);

    let mut workbook: Xlsx<_> = open_workbook(filepath)?;
    let sheet = workbook.worksheet_range_at(0)
        .ok_or("No sheet found")??;

    let height = sheet.height();
    let width = sheet.width();

    // Initialize counters
    let mut column_empty = vec![0; width];
    let mut row_empty = vec![0; height];

    // Count empty cells
    for (row_idx, row) in sheet.rows().enumerate() {
        for (col_idx, cell) in row.iter().enumerate() {
            if cell.is_empty() {
                column_empty[col_idx] += 1;
                row_empty[row_idx] += 1;
            }
        }
    }

    // Calculate column percentages
    let mut column_percentages: Vec<(String, f64)> = column_empty.iter().enumerate()
        .map(|(idx, &empty)| {
            let percentage = (empty as f64 / height as f64) * 100.0;
            (column_name(idx), percentage)
        })
        .collect();

    // Calculate row percentages
    let mut row_percentages: Vec<(String, f64)> = row_empty.iter().enumerate()
        .map(|(idx, &empty)| {
            let percentage = (empty as f64 / width as f64) * 100.0;
            (format!("Row {}", idx + 1), percentage)
        })
        .collect();

    // Sort percentages in descending order
    column_percentages.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    row_percentages.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    let total_cells = width * height;

    println!("Analysis completed in {:?}", timer.elapsed());

    Ok((
        EmptyAnalysis {
            empty_percentages: column_percentages,
            total_cells,
        },
        EmptyAnalysis {
            empty_percentages: row_percentages,
            total_cells,
        }
    ))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let files = HashMap::from([
        ("file1", "/home/aricept094/mydata/endometriosis/merged_endometriosis_data.xlsx"),

    ]);

    for (file_name, file_path) in files {
        println!("\nAnalyzing {}", file_name);
        match analyze_excel(file_path) {
            Ok((column_analysis, row_analysis)) => {
                // Create filenames for CSV output
                let column_filename = format!("/home/aricept094/mydata/endometriosis/{}_columns_analysis.csv", file_name);
                let row_filename = format!("/home/aricept094/mydata/endometriosis/{}_rows_analysis.csv", file_name);

                // Save results to CSV files
                save_to_csv(&column_analysis, &column_filename)?;
                save_to_csv(&row_analysis, &row_filename)?;

                println!("Results saved to:");
                println!("- {}", column_filename);
                println!("- {}", row_filename);
                println!("Total cells analyzed: {}", column_analysis.total_cells);
            }
            Err(e) => println!("Error analyzing {}: {}", file_name, e),
        }
    }

    Ok(())
}