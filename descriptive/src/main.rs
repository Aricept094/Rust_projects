use csv::Reader;
use serde::Deserialize;
use statrs::statistics::{Data, Distribution, OrderStatistics};
use std::error::Error;
use std::fs::File;

#[derive(Debug, Deserialize)]
struct Record {
    coef_a0: f64,
    coef_am1: f64,
    coef_bm1: f64,
    coef_am2: f64,
    coef_bm2: f64,
    coef_am3: f64,
    coef_bm3: f64,
    coef_am4: f64,
    coef_bm4: f64,
    coef_am5: f64,
    coef_bm5: f64,
}

fn calculate_statistics(data: &[f64]) -> Result<Statistics, Box<dyn Error>> {
    let mut sorted_data = data.to_vec();
    sorted_data.sort_by(|a, b| a.partial_cmp(b).unwrap());
    
    let mut data_stats = Data::new(data.to_vec());
    
    // Calculate quartiles
    let q1_idx = (data.len() as f64 * 0.25).floor() as usize;
    let q3_idx = (data.len() as f64 * 0.75).floor() as usize;
    
    Ok(Statistics {
        mean: data_stats.mean().unwrap(),
        median: data_stats.median(),
        std_dev: data_stats.std_dev().unwrap(),
        range: Range {
            min: *data.iter().min_by(|a, b| a.partial_cmp(b).unwrap()).unwrap(),
            max: *data.iter().max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap(),
        },
        iqr: sorted_data[q3_idx] - sorted_data[q1_idx],
        skewness: calculate_skewness(data),
        kurtosis: calculate_kurtosis(data),
    })
}


#[derive(Debug)]
struct Statistics {
    mean: f64,
    median: f64,
    std_dev: f64,
    range: Range,
    iqr: f64,
    skewness: f64,
    kurtosis: f64,
}

#[derive(Debug)]
struct Range {
    min: f64,
    max: f64,
}

fn calculate_skewness(data: &[f64]) -> f64 {
    let n = data.len() as f64;
    let mean = data.iter().sum::<f64>() / n;
    let variance = data.iter()
        .map(|x| (x - mean).powi(2))
        .sum::<f64>() / n;
    let std_dev = variance.sqrt();
    
    data.iter()
        .map(|x| ((x - mean) / std_dev).powi(3))
        .sum::<f64>() / n
}

fn calculate_kurtosis(data: &[f64]) -> f64 {
    let n = data.len() as f64;
    let mean = data.iter().sum::<f64>() / n;
    let variance = data.iter()
        .map(|x| (x - mean).powi(2))
        .sum::<f64>() / n;
    let std_dev = variance.sqrt();
    
    (data.iter()
        .map(|x| ((x - mean) / std_dev).powi(4))
        .sum::<f64>() / n) - 3.0  // Excess kurtosis
}

fn main() -> Result<(), Box<dyn Error>> {
    let file_path = "/home/aricept094/python/fourier_analysis_1d_meridian_results('Meridian_Angle_Rad')['Elevation_Anterior_Scaled']_all_patinets.csv";
    let file = File::open(file_path)?;
    let mut rdr = Reader::from_reader(file);
    
    let mut records: Vec<Record> = Vec::new();
    for result in rdr.deserialize() {
        let record: Record = result?;
        records.push(record);
    }
    
    // Extract individual coefficients into separate vectors
    let coef_names = vec![
        "coef_a0", "coef_am1", "coef_bm1", "coef_am2", "coef_bm2",
        "coef_am3", "coef_bm3", "coef_am4", "coef_bm4", "coef_am5", "coef_bm5"
    ];
    
    for (i, coef_name) in coef_names.iter().enumerate() {
        let data: Vec<f64> = match i {
            0 => records.iter().map(|r| r.coef_a0).collect(),
            1 => records.iter().map(|r| r.coef_am1).collect(),
            2 => records.iter().map(|r| r.coef_bm1).collect(),
            3 => records.iter().map(|r| r.coef_am2).collect(),
            4 => records.iter().map(|r| r.coef_bm2).collect(),
            5 => records.iter().map(|r| r.coef_am3).collect(),
            6 => records.iter().map(|r| r.coef_bm3).collect(),
            7 => records.iter().map(|r| r.coef_am4).collect(),
            8 => records.iter().map(|r| r.coef_bm4).collect(),
            9 => records.iter().map(|r| r.coef_am5).collect(),
            10 => records.iter().map(|r| r.coef_bm5).collect(),
            _ => unreachable!(),
        };
        
        let stats = calculate_statistics(&data)?;
        println!("\nStatistics for {}:", coef_name);
        println!("Mean: {:.4}", stats.mean);
        println!("Median: {:.4}", stats.median);
        println!("Standard Deviation: {:.4}", stats.std_dev);
        println!("Range: {:.4} to {:.4}", stats.range.min, stats.range.max);
        println!("Interquartile Range: {:.4}", stats.iqr);
        println!("Skewness: {:.4}", stats.skewness);
        println!("Kurtosis: {:.4}", stats.kurtosis);
    }
    
    Ok(())
}