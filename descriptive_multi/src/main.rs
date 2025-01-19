use csv::{Reader, Writer};
use serde::Deserialize;
use statrs::statistics::{Data, Distribution};
use std::error::Error;
use std::fs::File;
use std::path::Path;
use glob::glob;

#[derive(Debug, Deserialize)]
struct Record {
    dc_component: Option<f64>,
    component_1_amplitude: Option<f64>,
    component_2_amplitude: Option<f64>,
    higher_order_amplitude_sum: Option<f64>,
    r2_score: Option<f64>,
}

#[derive(Debug)]
struct Statistics {
    mean: f64,
    std_dev: f64,
    range: Range,
}

#[derive(Debug)]
struct Range {
    min: f64,
    max: f64,
}

fn calculate_statistics(data: &[f64]) -> Result<Statistics, Box<dyn Error>> {
    let data_stats = Data::new(data.to_vec());

    Ok(Statistics {
        mean: data_stats.mean().unwrap(),
        std_dev: data_stats.std_dev().unwrap(),
        range: Range {
            min: *data.iter().min_by(|a, b| a.partial_cmp(b).unwrap()).unwrap(),
            max: *data.iter().max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap(),
        },
    })
}

fn analyze_file(file_path: &Path) -> Result<Vec<(String, Statistics)>, Box<dyn Error>> {
    let file = File::open(file_path)?;
    let mut rdr = Reader::from_reader(file);

    let records: Vec<Record> = rdr.deserialize().collect::<Result<_, _>>()?;

    // Filter out None values and collect only valid f64 values
    let columns = vec![
        ("dc_component", records.iter().filter_map(|r| r.dc_component).collect::<Vec<_>>()),
        ("component_1_amplitude", records.iter().filter_map(|r| r.component_1_amplitude).collect()),
        ("component_2_amplitude", records.iter().filter_map(|r| r.component_2_amplitude).collect()),
        ("higher_order_amplitude_sum", records.iter().filter_map(|r| r.higher_order_amplitude_sum).collect()),
        ("r2_score", records.iter().filter_map(|r| r.r2_score).collect()),
    ];

    let stats: Vec<_> = columns.into_iter()
        .filter(|(_, data)| !data.is_empty()) // Skip columns with no valid data
        .map(|(name, data)| {
            let stats = calculate_statistics(&data).unwrap();
            (name.to_string(), stats)
        })
        .collect();

    Ok(stats)
}

fn main() -> Result<(), Box<dyn Error>> {
    let dir_path = "/home/aricept094/mydata/sheets/combined_data/radial_results";
    let pattern = format!("{}/*.csv", dir_path);
    
    let mut wtr = Writer::from_path("analysis_results2.csv")?;
    
    // Write header
    wtr.write_record(&[
        "File", "Column", "Mean", "Std Dev", "Range"
    ])?;

    // Process each file
    for entry in glob(&pattern)? {
        match entry {
            Ok(path) => {
                let file_name = path.file_name().unwrap().to_string_lossy();
                println!("Processing file: {}", file_name);

                match analyze_file(&path) {
                    Ok(stats) => {
                        for (column_name, stat) in stats {
                            let record = vec![
                                file_name.to_string(),
                                column_name,
                                format!("{:.4}", stat.mean),
                                format!("{:.4}", stat.std_dev),
                                format!("[{:.4} - {:.4}]", stat.range.min, stat.range.max),
                            ];
                            wtr.write_record(&record)?;
                        }
                    },
                    Err(e) => eprintln!("Error processing file {}: {}", file_name, e),
                }
            },
            Err(e) => eprintln!("Error accessing file: {}", e),
        }
    }

    wtr.flush()?;
    println!("Analysis complete. Results saved to analysis_results.csv");
    Ok(())
}