use std::collections::HashSet;
use std::error::Error;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use csv::{ReaderBuilder, WriterBuilder};
use encoding_rs::UTF_8;
use encoding_rs_io::DecodeReaderBytesBuilder;

struct ColumnStats {
    name: String,
    unique_count: usize,
    missing_count: usize,
    zero_count: usize,
    total_rows: usize,
    quality_score: f64,
    recommendation: String,
}

fn calculate_quality_score(stats: &ColumnStats) -> f64 {
    let non_missing_rows = stats.total_rows - stats.missing_count;
    if non_missing_rows == 0 {
        return 0.0;
    }

    // Calculate percentages
    let missing_percentage = stats.missing_count as f64 / stats.total_rows as f64;
    let zero_percentage = stats.zero_count as f64 / stats.total_rows as f64;
    
    // Calculate cardinality score (penalize very low unique values)
    let cardinality_score = if stats.unique_count <= 2 {
        0.2  // Severe penalty for binary columns
    } else if stats.unique_count <= 5 {
        0.4  // Significant penalty for very low cardinality
    } else {
        // For higher cardinality, use a logarithmic scale
        let max_expected_unique = (non_missing_rows as f64).sqrt();  // Square root as reasonable max unique values
        let unique_ratio = (stats.unique_count as f64).min(max_expected_unique) / max_expected_unique;
        unique_ratio.powf(0.5)  // Use square root to make the scale more gradual
    };

    // Weights for different factors
    const MISSING_WEIGHT: f64 = 0.35;
    const ZERO_WEIGHT: f64 = 0.30;
    const CARDINALITY_WEIGHT: f64 = 0.35;

    // Calculate score (higher is better)
    let score = (1.0 - missing_percentage) * MISSING_WEIGHT +
                (1.0 - zero_percentage) * ZERO_WEIGHT +
                cardinality_score * CARDINALITY_WEIGHT;

    // Scale to 0-100
    (score * 100.0).round()
}

fn get_recommendation(stats: &ColumnStats) -> String {
    let non_missing_rows = stats.total_rows - stats.missing_count;
    let missing_percentage = (stats.missing_count as f64 / stats.total_rows as f64 * 100.0).round();
    let zero_percentage = (stats.zero_count as f64 / stats.total_rows as f64 * 100.0).round();
    let non_zero_percentage = ((non_missing_rows - stats.zero_count) as f64 / stats.total_rows as f64 * 100.0).round();

    if missing_percentage > 50.0 {
        return "High missing values - Consider excluding".to_string();
    } else if zero_percentage > 70.0 {
        return "Mostly zeros - Consider excluding or special handling".to_string();
    } else if stats.unique_count == 1 {
        return "Single value column - No variability".to_string();
    } else if stats.unique_count == 2 {
        return "Binary column - Very low variability".to_string();
    } else if stats.unique_count <= 5 {
        return "Low cardinality column - Limited variability".to_string();
    } else if non_zero_percentage < 20.0 {
        return "Low information content - Review necessity".to_string();
    } else if stats.quality_score > 80.0 {
        return "Good quality - Use as is".to_string();
    } else if stats.quality_score > 60.0 {
        return "Moderate quality - Consider cleaning".to_string();
    } else {
        return "Poor quality - Needs investigation".to_string();
    }
}

fn analyze_csv(file_path: &str, output_path: &str) -> Result<(), Box<dyn Error>> {
    // Open the input CSV file with UTF-8 BOM sniffing
    let file = File::open(file_path)?;
    let transcoded_reader = DecodeReaderBytesBuilder::new()
        .encoding(None)
        .build(file);

    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .from_reader(transcoded_reader);

    let headers = reader.headers()?.clone();

    let mut target_columns = Vec::new();
    for (index, header) in headers.iter().enumerate() {
        if header.contains("فولیکول") || header.contains("فولیکل") {
            target_columns.push(index);
        }
    }

    if target_columns.is_empty() {
        println!("No columns found with 'تعداد فولیکول' or 'فولیکل' in their header name.");
        return Ok(());
    }

    let mut results = Vec::new();

    for &column_index in &target_columns {
        let mut unique_values = HashSet::new();
        let mut missing_count = 0;
        let mut zero_count = 0;
        let mut total_rows = 0;

        let file = File::open(file_path)?;
        let transcoded_reader = DecodeReaderBytesBuilder::new()
            .encoding(None)
            .build(file);
        let mut reader = ReaderBuilder::new()
            .flexible(true)
            .from_reader(transcoded_reader);
        reader.headers()?;

        for record_result in reader.records() {
            let record = record_result?;
            total_rows += 1;
            
            if let Some(value) = record.get(column_index) {
                let trimmed_value = value.trim();
                if trimmed_value.is_empty() 
                    || trimmed_value == " "
                    || trimmed_value == "  "
                    || trimmed_value == "   "
                    || trimmed_value == "    " {
                    missing_count += 1;
                } else {
                    // Check for zero values (including "0", "0.0", "0.00", etc.)
                    if trimmed_value.chars().all(|c| c == '0' || c == '.') {
                        zero_count += 1;
                    }
                    unique_values.insert(value.to_string());
                }
            } else {
                missing_count += 1;
            }
        }

        let column_stats = ColumnStats {
            name: headers.get(column_index).unwrap_or("Unknown Column").to_string(),
            unique_count: unique_values.len(),
            missing_count,
            zero_count,
            total_rows,
            quality_score: 0.0, // Placeholder, will be calculated
            recommendation: String::new(), // Placeholder, will be calculated
        };

        let mut final_stats = column_stats;
        final_stats.quality_score = calculate_quality_score(&final_stats);
        final_stats.recommendation = get_recommendation(&final_stats);
        
        results.push(final_stats);
    }

    // Sort results by quality score in descending order
    results.sort_by(|a, b| b.quality_score.partial_cmp(&a.quality_score).unwrap());

    // Create output file and write UTF-8 BOM
    let mut file = File::create(output_path)?;
    file.write_all(&[0xEF, 0xBB, 0xBF])?;

    let mut writer = WriterBuilder::new()
        .has_headers(true)
        .from_writer(file);

    writer.write_record(&[
        "Column Name",
        "Quality Score",
        "Unique Value Count",
        "Missing Value Count",
        "Zero Value Count",
        "Total Rows",
        "Missing %",
        "Zero %",
        "Valid %",
        "Recommendation"
    ])?;

    for stats in results {
        let missing_percentage = (stats.missing_count as f64 / stats.total_rows as f64 * 100.0).round();
        let zero_percentage = (stats.zero_count as f64 / stats.total_rows as f64 * 100.0).round();
        let valid_percentage = ((stats.total_rows - stats.missing_count - stats.zero_count) as f64 
            / stats.total_rows as f64 * 100.0).round();

        writer.write_record(&[
            stats.name,
            format!("{:.1}", stats.quality_score),
            stats.unique_count.to_string(),
            stats.missing_count.to_string(),
            stats.zero_count.to_string(),
            stats.total_rows.to_string(),
            format!("{}%", missing_percentage),
            format!("{}%", zero_percentage),
            format!("{}%", valid_percentage),
            stats.recommendation,
        ])?;
    }

    writer.flush()?;
    println!("Results saved to {}", output_path);
    Ok(())
}

fn main() {
    let input_file_path = "/home/aricept094/mydata/PCO/merged_pco_data_cleaned.csv";
    let output_file_path = "analysis_results.csv";

    if !Path::new(input_file_path).exists() {
        println!("Error: Input file not found at {}", input_file_path);
        return;
    }

    if let Err(err) = analyze_csv(input_file_path, output_file_path) {
        println!("Error analyzing CSV: {}", err);
    }
}