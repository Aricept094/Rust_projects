use std::error::Error;
use std::fs;
use std::path::Path;
use std::collections::HashMap;
use std::sync::Mutex;
use csv::{ReaderBuilder, WriterBuilder};
use rayon::prelude::*;

#[derive(Clone)]
struct Stats {
    mean: f64,
    std_dev: f64,
}

fn bessel_j0(x: f64) -> f64 {
    if x == 0.0 {
        return 1.0;
    }
    (x.sin() / x).cos()
}

fn fourier_bessel_transform(radial_index: usize, num_radials: usize) -> f64 {
    let r_max = 1.0;
    let r = (radial_index as f64) / (num_radials as f64 - 1.0);
    let alpha = std::f64::consts::PI;
    let transformed_r = r * alpha / r_max;
    bessel_j0(transformed_r)
}

fn calculate_stats(values: &[f64]) -> Result<Stats, Box<dyn Error + Send + Sync>> {
    if values.is_empty() {
        return Ok(Stats { mean: 0.0, std_dev: 0.0 });
    }

    if values.iter().any(|x| x.is_nan()) {
        return Err("Dataset contains NaN values".into());
    }

    let count = values.len() as f64;
    
    let mean = values.iter()
        .fold(0.0, |acc, &x| acc + x / count);

    if !mean.is_finite() {
        return Err("Mean calculation resulted in non-finite value".into());
    }

    let variance = if values.len() > 1 {
        values.iter()
            .fold(0.0, |acc, &x| {
                let diff = x - mean;
                acc + (diff * diff) / (count - 1.0)
            })
    } else {
        0.0
    };

    if !variance.is_finite() || variance < 0.0 {
        return Err("Variance calculation resulted in invalid value".into());
    }

    let std_dev = variance.sqrt();

    Ok(Stats { mean, std_dev })
}

fn read_parameter_file(file_path: &Path) -> Result<Vec<f64>, Box<dyn Error + Send + Sync>> {
    let mut values = Vec::new();
    let mut rdr = ReaderBuilder::new()
        .has_headers(false)
        .from_path(file_path)?;

    for result in rdr.records() {
        let record = result?;
        for value_str in record.iter() {
            let value: f64 = value_str.parse()?;
            if !value.is_finite() {
                return Err("File contains non-finite values".into());
            }
            values.push(value);
        }
    }
    Ok(values)
}

fn scale_value(value: f64, stats: &Stats) -> f64 {
    if !value.is_finite() || !stats.mean.is_finite() || !stats.std_dev.is_finite() {
        return 0.0;
    }

    if stats.std_dev <= 0.0 {
        return 0.0;
    }

    (value - stats.mean) / stats.std_dev
}

fn process_patient_data(
    base_dir: &Path,
    patient_id: &str,
    output_dir: &Path
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let num_meridians = 256;
    let num_radials = 32;

    let mut stats_map = HashMap::new();
    let mut parameters = vec![
        ("Axial_Anterior", Vec::new()),
        ("Axial_Posterior", Vec::new()),
        ("Elevation_Anterior", Vec::new()),
        ("Elevation_Posterior", Vec::new()),
        ("Axial_Keratometric", Vec::new()),
        ("Height_Anterior", Vec::new()),
        ("Height_Posterior", Vec::new()),
        ("Pachymetry", Vec::new()),
    ];

    for (param_name, param_data) in parameters.iter_mut() {
        let folder_name = param_name.replace("_", " ");
        let file_path = base_dir
            .join(&folder_name)
            .join(format!("{}_{}.csv", param_name, patient_id));
        
        println!("Reading file: {:?}", file_path);
        
        *param_data = read_parameter_file(&file_path)?;
        let stats = calculate_stats(param_data)?;
        let stats_clone = stats.clone();
        stats_map.insert(param_name.to_string(), stats);
        
        println!("Stats for {}: Mean = {:.6}, StdDev = {:.6}", 
                param_name, stats_clone.mean, stats_clone.std_dev);
    }

    let output_path = output_dir.join(format!("{}_combined.csv", patient_id));
    let wtr = Mutex::new(WriterBuilder::new()
        .has_headers(true)
        .from_path(&output_path)?);

    let mut header = vec![
        "Meridian_Index".to_string(),
        "Radial_Index".to_string(),
        "Meridian_Angle_Deg".to_string(),
        "Meridian_Angle_Rad".to_string(),
        "Normalized_Radius".to_string(),
        "Transformed_Radius".to_string(),
        "Cos_Theta".to_string(),
        "Sin_Theta".to_string(),
        "X_Coordinate".to_string(),
        "Y_Coordinate".to_string(),
        "Alpha_Angle".to_string(), // Add new column for alpha_angle
    ];

    for (param_name, _) in &parameters {
        header.push(format!("{}_Value", param_name));
        header.push(format!("{}_Scaled", param_name));
    }

    wtr.lock().unwrap().write_record(&header)?;

    let parameters = parameters.clone();
    let stats_map = stats_map.clone();

    let rows: Vec<_> = (0..num_meridians).into_par_iter().flat_map(move |meridian| {
        let parameters = parameters.clone();
        let stats_map = stats_map.clone();
        
        (0..num_radials).into_par_iter().map(move |radial_index| {
            let radial_index_1_based = radial_index + 1;
            let meridian_index_1_based = meridian + 1;
            let data_index = meridian * num_radials + radial_index;
            
            let meridian_angle_deg = (meridian_index_1_based as f64 - 1.0) 
                * (360.0 / num_meridians as f64);
            let meridian_angle_rad = meridian_angle_deg.to_radians();
            let normalized_radius = (radial_index_1_based as f64 - 1.0) 
                / (num_radials as f64 - 1.0);
            
            let transformed_radius = fourier_bessel_transform(radial_index_1_based, num_radials);
            
            let cos_theta = meridian_angle_rad.cos();
            let sin_theta = meridian_angle_rad.sin();
            
            let x_coordinate = transformed_radius * cos_theta;
            let y_coordinate = transformed_radius * sin_theta;
            
            // Calculate alpha_angle
            let pachymetry = parameters.iter()
                .find(|(name, _)| *name == "Pachymetry")
                .map(|(_, data)| data[data_index])
                .unwrap_or(0.0);

            let height_posterior = parameters.iter()
                .find(|(name, _)| *name == "Height_Posterior")
                .map(|(_, data)| data[data_index])
                .unwrap_or(0.0);

            let height_anterior = parameters.iter()
                .find(|(name, _)| *name == "Height_Anterior")
                .map(|(_, data)| data[data_index])
                .unwrap_or(0.0);

            let height_diff = height_posterior - height_anterior;
            let alpha_angle = if height_diff != 0.0 {
                pachymetry / height_diff
            } else {
                f64::NAN // Handle division by zero
            };
            
            let mut row = vec![
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
                alpha_angle.to_string(), // Add alpha_angle to the output
            ];
            
            for (param_name, param_data) in &parameters {
                let value = param_data[data_index];
                let stats = stats_map.get(*param_name).unwrap();
                let scaled = scale_value(value, stats);
                
                row.push(value.to_string());
                row.push(scaled.to_string());
            }
            
            row
        }).collect::<Vec<_>>()
    }).collect();

    for row in rows {
        wtr.lock().unwrap().write_record(&row)?;
    }

    println!("Created combined file: {:?}", output_path);
    Ok(())
}


fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let base_dir = Path::new("/home/aricept094/mydata/casia_less_than_1/processed_data");
    let output_dir = Path::new("/home/aricept094/mydata/casia_less_than_1/combined_data");

    println!("Creating output directory: {:?}", output_dir);
    fs::create_dir_all(output_dir)?;

    let sample_dir = base_dir.join("Elevation Anterior");
    let mut patient_ids = Vec::new();

    println!("Scanning directory: {:?}", sample_dir);

    for entry in fs::read_dir(sample_dir)? {
        let entry = entry?;
        let path = entry.path();
        
        if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
            if file_name.ends_with(".csv") {
                if let Some(id) = file_name
                    .strip_prefix("Elevation_Anterior_")
                    .and_then(|s| s.strip_suffix(".csv"))
                {
                    patient_ids.push(id.to_string());
                    println!("Found patient ID: {}", id);
                }
            }
        }
    }

    println!("Found {} patients to process", patient_ids.len());

    patient_ids.par_iter().enumerate().try_for_each(|(i, patient_id)| {
        println!("\nProcessing patient {}/{}: {}", 
                i + 1, patient_ids.len(), patient_id);
        process_patient_data(base_dir, patient_id, output_dir)
    })?;

    println!("\nAll patients processed successfully!");
    Ok(())
}