use calamine::{open_workbook_auto, Reader, DataType};
use std::path::Path;

fn main() {
    // Specify the path to your Excel file
    let path = Path::new("/home/aricept094/mydata/First_Rabbit_series.xlsx");

    // Open the Excel file
    let mut workbook = open_workbook_auto(path).expect("Cannot open Excel file");

    // Iterate over all sheets in the workbook
    for sheet_name in workbook.sheet_names().to_owned() {
        println!("Sheet: {}", sheet_name);

        // Read the sheet
        if let Some(Ok(range)) = workbook.worksheet_range(&sheet_name) {
            // Get the first row (headings)
            if let Some(first_row) = range.rows().next() {
                let headings: Vec<String> = first_row.iter()
                    .map(|cell| match cell {
                        DataType::String(s) => s.clone(),
                        DataType::Int(i) => i.to_string(),
                        DataType::Float(f) => f.to_string(),
                        DataType::Bool(b) => b.to_string(),
                        DataType::Error(e) => format!("Error({:?})", e),
                        DataType::Empty => String::from("Empty"),
                        DataType::DateTime(_) => String::from("DateTime"),
                        DataType::DateTimeIso(_) => String::from("DateTimeIso"),
                        DataType::DurationIso(_) => String::from("DurationIso"),
                    })
                    .collect();

                println!("Headings: {:?}", headings);
            }
        }
    }
}