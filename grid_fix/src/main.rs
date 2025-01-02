use std::error::Error;
use std::fs::File;
use std::path::Path;
use csv::{ReaderBuilder, Writer};
use serde::{Deserialize, Serialize};
use std::f64::consts::PI;

#[derive(Debug, Serialize)]
struct AnalysisPoint {
    meridian: f64,
    radius: f64,
    keratometry: f64,
    x: f64,
    y: f64,
}

fn calculate_coordinates(meridian_degrees: f64, radius: f64) -> (f64, f64) {
    let meridian_rad = meridian_degrees * PI / 180.0;
    let x = (radius * meridian_rad.cos() * 1000.0).round() / 1000.0;  // Round to 3 decimal places
    let y = (radius * meridian_rad.sin() * 1000.0).round() / 1000.0;
    (x, y)
}

fn main() -> Result<(), Box<dyn Error>> {
    // File paths
    let input_path = Path::new("/home/aricept094/mydata/testrm.csv");
    let output_path = Path::new("/home/aricept094/mydata/analysis_format.csv");

    // Read input CSV
    let mut reader = ReaderBuilder::new()
        .has_headers(false)
        .from_path(input_path)?;

    // Initialize vectors to store data
    let mut grid_data: Vec<Vec<f64>> = Vec::new();
    
    // Read CSV into grid
    for result in reader.records() {
        let record = result?;
        let row: Vec<f64> = record.iter()
            .map(|field| field.parse::<f64>().unwrap_or(0.0))
            .collect();
        grid_data.push(row);
    }

    // Create writer for output CSV
    let mut writer = Writer::from_path(output_path)?;
    
    // Write header
    writer.write_record(&["Meridian", "Radius", "Keratometry", "X", "Y"])?;

    // Generate meridians (32 points from 0 to 360 degrees)
    let meridians: Vec<f64> = (0..32)
        .map(|i| (i as f64 * 11.25))  // Exact 11.25° steps
        .collect();

    // Generate radial distances (256 points from 0 to 1)
    let radial_distances: Vec<f64> = (0..256)
        .map(|i| (i as f64 / 255.0).round() * 1000.0 / 1000.0)  // Ensure precise division
        .collect();

    // Transform data
    for (j, &radius) in radial_distances.iter().enumerate() {
        for (i, &meridian) in meridians.iter().enumerate() {
            // Get keratometry value from grid
            let keratometry = grid_data[j][i];

            // Calculate precise X and Y coordinates
            let (x, y) = calculate_coordinates(meridian, radius);

            // Create analysis point
            let point = AnalysisPoint {
                meridian,
                radius,
                keratometry,
                x,
                y,
            };

            // Write point to CSV with proper formatting
            writer.write_record(&[
                format!("{:.2}", point.meridian),
                format!("{:.6}", point.radius),
                format!("{:.8}", point.keratometry),
                format!("{:.6}", point.x),
                format!("{:.6}", point.y),
            ])?;
        }
    }

    // Verify first few points
    println!("Verification of first few points:");
    println!("Meridian\tRadius\tX\tY");
    for &meridian in meridians.iter().take(5) {
        let (x, y) = calculate_coordinates(meridian, radial_distances[1]);  // Check second radius
        println!("{:.2}\t{:.6}\t{:.6}\t{:.6}", 
                meridian, 
                radial_distances[1], 
                x, 
                y);
    }

    println!("\nTransformation complete! Output saved to: {:?}", output_path);
    Ok(())
}