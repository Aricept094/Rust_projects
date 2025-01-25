use csv::{Reader, WriterBuilder};
use serde::Deserialize;
use statrs::statistics::{Data, Distribution};
use std::error::Error;
use std::fs::{File, OpenOptions};
use std::path::Path;
use glob::glob;
use rayon::prelude::*;

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

fn get_radius_label(filename: &str) -> String {
    match filename {
        f if f.contains("radial_4") => "radius 0.5mm".to_string(),
        f if f.contains("radial_8") => "radius 1mm".to_string(),
        f if f.contains("radial_12") => "radius 1.5mm".to_string(),
        f if f.contains("radial_16") => "radius 2mm".to_string(),
        f if f.contains("radial_20") => "radius 2.5mm".to_string(),
        f if f.contains("radial_24") => "radius 3mm".to_string(),
        _ => "unknown radius".to_string(),
    }
}

fn analyze_file(file_path: &Path) -> Result<Vec<(String, Statistics)>, Box<dyn Error>> {
    let file = File::open(file_path)?;
    let mut rdr = Reader::from_reader(file);
    let records: Vec<Record> = rdr.deserialize().collect::<Result<_, _>>()?;

    let columns = vec![
        ("dc_component", records.iter().filter_map(|r| r.dc_component).collect::<Vec<_>>()),
        ("component_1_amplitude", records.iter().filter_map(|r| r.component_1_amplitude).collect()),
        ("component_2_amplitude", records.iter().filter_map(|r| r.component_2_amplitude).collect()),
        ("higher_order_amplitude_sum", records.iter().filter_map(|r| r.higher_order_amplitude_sum).collect()),
        ("r2_score", records.iter().filter_map(|r| r.r2_score).collect()),
    ];

    let stats: Vec<_> = columns.into_iter()
        .filter(|(_, data)| !data.is_empty())
        .map(|(name, data)| {
            let stats = calculate_statistics(&data).unwrap();
            (name.to_string(), stats)
        })
        .collect();

    Ok(stats)
}

fn format_statistics(stat: &Statistics) -> String {
    // Using Unicode escape sequence for ± symbol
    format!("{:.4} \u{00B1} {:.4} [{:.4} - {:.4}]",
            stat.mean,
            stat.std_dev,
            stat.range.min,
            stat.range.max
    )
}

fn main() -> Result<(), Box<dyn Error>> {
    let dir_path = "/home/aricept094/mydata/sheets/combined_data/radial_results/sheets/Elevation_Posterior_Value";
    let pattern = format!("{}/*.csv", dir_path);


    // Collect paths first to parallelize
    let paths: Vec<_> = glob(&pattern)?.filter_map(Result::ok).collect();

    // Process each file in parallel using rayon and collect results
    let results: Vec<Vec<(String, String, Statistics)>> = paths.par_iter()
        .map(|path| {
            let file_name = path.file_name().unwrap().to_string_lossy();
            let radius_label = get_radius_label(&file_name);
            println!("Processing file: {} ({})", file_name, radius_label);

            match analyze_file(path) {
                Ok(stats) => {
                    stats.into_iter().map(|(column_name, stat)| (radius_label.clone(), column_name, stat)).collect()
                },
                Err(e) => {
                    eprintln!("Error processing file {}: {}", file_name, e);
                    Vec::new() // Return empty Vec in case of error to continue processing other files
                },
            }
        })
        .collect();

    // Flatten the results from parallel processing
    let mut all_results: Vec<(String, String, Statistics)> = results.into_iter().flatten().collect();


    // Sort results
    let radius_order = vec![
        "radius 0.5mm",
        "radius 1mm",
        "radius 1.5mm",
        "radius 2mm",
        "radius 2.5mm",
        "radius 3mm"
    ];

    all_results.sort_by(|a, b| {
        let radius_a_pos = radius_order.iter().position(|&r| r == a.0);
        let radius_b_pos = radius_order.iter().position(|&r| r == b.0);
        radius_a_pos.cmp(&radius_b_pos)
            .then_with(|| a.1.cmp(&b.1))
    });

    // Write BOM for UTF-8
    std::fs::write("analysis_results_sheets_Elevation_Posterior_Value.csv", [0xEF, 0xBB, 0xBF])?;

    // Create final writer
    let mut final_wtr = WriterBuilder::new()
        .has_headers(false)
        .from_writer(OpenOptions::new()
            .write(true)
            .append(true)
            .open("analysis_results_sheets_Elevation_Posterior_Value.csv")?);

    // Write headers
    final_wtr.write_record(&["Radius", "Column", "Statistics"])?;

    // Write sorted results
    for (radius, column_name, stat) in all_results {
        let record = vec![
            radius,
            column_name,
            format_statistics(&stat),
        ];
        final_wtr.write_record(&record)?;
    }

    final_wtr.flush()?;
    println!("Analysis complete. Results saved to analysis_results_Elevation_Posterior_Value.csv");
    Ok(())
}