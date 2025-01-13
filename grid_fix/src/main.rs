use std::error::Error;
use std::fs;
use std::path::Path;
use csv::{ReaderBuilder, WriterBuilder};
use std::f64::consts::PI;

// Add Bessel function calculation (simplified first-order)
fn bessel_j0(x: f64) -> f64 {
    if x == 0.0 {
        return 1.0;
    }
    (x.sin() / x).cos()
}

// New function for Fourier-Bessel transform of radial indices
fn fourier_bessel_transform(radial_index: usize, num_radials: usize) -> f64 {
    let r_max = 1.0; // Normalized maximum radius
    let r = (radial_index as f64) / (num_radials as f64 - 1.0);
    
    // Calculate the transformed radius using Fourier-Bessel
    let alpha = PI; // First zero of J0
    let transformed_r = r * alpha / r_max;
    bessel_j0(transformed_r)
}

struct Stats {
    mean: f64,
    std_dev: f64,
}

fn calculate_stats(values: &[f64]) -> Stats {
    let sum: f64 = values.iter().sum();
    let count = values.len() as f64;
    let mean = sum / count;
    
    let variance: f64 = if values.len() > 1 {
        values.iter()
            .map(|x| (*x - mean).powi(2))
            .sum::<f64>() / (count - 1.0)
    } else {
        0.0
    };
    let std_dev = variance.sqrt();
    
    Stats { mean, std_dev }
}

fn process_csv_file(input_path: &Path, output_path: &Path) -> Result<(), Box<dyn Error>> {
    let num_meridians = 256;
    let num_radials = 32;
    
    let mut k_values = Vec::new();
    let mut rdr = ReaderBuilder::new()
        .has_headers(false)
        .from_path(input_path)?;
    
    for result in rdr.records() {
        let record = result?;
        for value_str in record.iter() {
            let k_reading: f64 = value_str.parse()?;
            k_values.push(k_reading);
        }
    }
    
    let stats = calculate_stats(&k_values);
    
    println!("File: {}", input_path.display());
    println!("Mean: {:.6}", stats.mean);
    println!("Standard Deviation: {:.6}", stats.std_dev);
    println!("Sample Size: {}", k_values.len());
    
    let mut rdr = ReaderBuilder::new()
        .has_headers(false)
        .from_path(input_path)?;
    
    let mut wtr = WriterBuilder::new()
        .has_headers(false)
        .from_path(output_path)?;
    
    wtr.write_record(&[
        "Meridian_Index",
        "Radial_Index",
        "Meridian_Angle_Deg",
        "Meridian_Angle_Rad",
        "Normalized_Radius",
        "Transformed_Radius",
        "Cos_Theta",
        "Sin_Theta",
        "X_Coordinate",
        "Y_Coordinate",
        "Keratometry_Value",
        "KR_scaled",
    ])?;
    
    let mut meridian_index_1_based = 0;
    for result in rdr.records() {
        meridian_index_1_based += 1;
        let record = result?;
        
        for (radial_index, value_str) in record.iter().enumerate() {
            let k_reading: f64 = value_str.parse()?;
            let radial_index_1_based = radial_index + 1;
            
            let meridian_angle_deg = (meridian_index_1_based as f64 - 1.0) 
                * (360.0 / num_meridians as f64);
            let meridian_angle_rad = meridian_angle_deg.to_radians();
            let normalized_radius = (radial_index_1_based as f64 - 1.0) 
                / (num_radials as f64 - 1.0);
            
            // Apply Fourier-Bessel transform to the radius
            let transformed_radius = fourier_bessel_transform(radial_index_1_based, num_radials);
            
            let cos_theta = meridian_angle_rad.cos();
            let sin_theta = meridian_angle_rad.sin();
            
            // Use transformed radius for coordinate calculation
            let x_coordinate = transformed_radius * cos_theta;
            let y_coordinate = transformed_radius * sin_theta;
            
            let kr_scaled = if stats.std_dev != 0.0 {
                (k_reading - stats.mean) / stats.std_dev
            } else {
                0.0
            };
            
            wtr.write_record(&[
                meridian_index_1_based.to_string(),
                radial_index_1_based.to_string(),
                meridian_angle_deg.to_string(),
                meridian_angle_rad.to_string(),
                normalized_radius.to_string(),
                transformed_radius.to_string(),
                cos_theta.to_string(),
                sin_theta.to_string(),
                x_coordinate.to_string(),
                y_coordinate.to_string(),
                k_reading.to_string(),
                kr_scaled.to_string(),
            ])?;
        }
    }
    
    wtr.flush()?;
    println!("Processed: {} -> {}\n", input_path.display(), output_path.display());
    
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let input_dir = Path::new("/home/aricept094/mydata/sheets/conv");
    let output_dir = Path::new("/home/aricept094/mydata/sheets/conv/transformed2");
    
    // Create output directory if it doesn't exist
    fs::create_dir_all(output_dir)?;
    
    // Process each CSV file in the input directory
    for entry in fs::read_dir(input_dir)? {
        let entry = entry?;
        let path = entry.path();
        
        // Skip if not a CSV file
        if path.extension().and_then(|s| s.to_str()) != Some("csv") {
            continue;
        }
        
        // Create output path with "transformed" added to filename
        let file_stem = path.file_stem()
            .and_then(|s| s.to_str())
            .ok_or("Invalid filename")?;
        let new_filename = format!("{}_transformed.csv", file_stem);
        let output_path = output_dir.join(new_filename);
        
        // Process the file
        process_csv_file(&path, &output_path)?;
    }
    
    println!("All CSV files have been processed successfully!");
    Ok(())
}