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
    one_count: usize,
    total_rows: usize,
    quality_score: f64,
    variability_percentage: f64,  // Added field for value variability
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
    let one_percentage = stats.one_count as f64 / stats.total_rows as f64;
    
    // Calculate cardinality score (penalize very low unique values)
    let cardinality_score = if stats.unique_count <= 2 {
        0.2
    } else if stats.unique_count <= 5 {
        0.4
    } else {
        let max_expected_unique = (non_missing_rows as f64).sqrt();
        let unique_ratio = (stats.unique_count as f64).min(max_expected_unique) / max_expected_unique;
        unique_ratio.powf(0.5)
    };

    const MISSING_WEIGHT: f64 = 0.35;
    const ZERO_AND_ONE_WEIGHT: f64 = 0.30;
    const CARDINALITY_WEIGHT: f64 = 0.35;

    let score = (1.0 - missing_percentage) * MISSING_WEIGHT +
                (1.0 - (zero_percentage + one_percentage)) * ZERO_AND_ONE_WEIGHT +
                cardinality_score * CARDINALITY_WEIGHT;

    (score * 100.0).round()
}

fn calculate_variability_percentage(stats: &ColumnStats) -> f64 {
    let non_missing_rows = stats.total_rows - stats.missing_count;
    if non_missing_rows == 0 {
        return 0.0;
    }

    // Calculate what percentage of non-missing values are unique
    let variability = (stats.unique_count as f64 / non_missing_rows as f64 * 100.0).round();
    
    // Cap at 100% and ensure we don't return negative values
    variability.min(100.0).max(0.0)
}

fn get_recommendation(stats: &ColumnStats) -> String {
    let non_missing_rows = stats.total_rows - stats.missing_count;
    let missing_percentage = (stats.missing_count as f64 / stats.total_rows as f64 * 100.0).round();
    let zero_percentage = (stats.zero_count as f64 / stats.total_rows as f64 * 100.0).round();
    let one_percentage = (stats.one_count as f64 / stats.total_rows as f64 * 100.0).round();
    let non_zero_one_percentage = ((non_missing_rows - stats.zero_count - stats.one_count) as f64 
        / stats.total_rows as f64 * 100.0).round();

    // Include variability in recommendations
    if missing_percentage > 50.0 {
        return "High missing values - Consider excluding".to_string();
    } else if zero_percentage + one_percentage > 70.0 {
        return "Mostly zeros and ones - Consider excluding or special handling".to_string();
    } else if stats.unique_count == 1 {
        return "Single value column - No variability".to_string();
    } else if stats.unique_count == 2 {
        return "Binary column - Very low variability".to_string();
    } else if stats.variability_percentage < 1.0 {
        return "Extremely low variability - Consider excluding".to_string();
    } else if stats.variability_percentage > 90.0 {
        return "High variability - Possible unique identifier".to_string();
    } else if stats.unique_count <= 5 {
        return "Low cardinality column - Limited variability".to_string();
    } else if non_zero_one_percentage < 20.0 {
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
    let file = File::open(file_path)?;
    let transcoded_reader = DecodeReaderBytesBuilder::new()
        .encoding(None)
        .build(file);

    let mut reader = ReaderBuilder::new()
        .flexible(true)
        .from_reader(transcoded_reader);

    let headers = reader.headers()?.clone();
    let column_count = headers.len();
    let mut results = Vec::new();

    for column_index in 0..column_count {
        let mut unique_values = HashSet::new();
        let mut missing_count = 0;
        let mut zero_count = 0;
        let mut one_count = 0;
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
                    if trimmed_value.chars().all(|c| c == '0' || c == '.') {
                        zero_count += 1;
                    }
                    else if trimmed_value == "1" || trimmed_value == "1.0" || trimmed_value == "1.00" {
                        one_count += 1;
                    }
                    unique_values.insert(value.to_string());
                }
            } else {
                missing_count += 1;
            }
        }

        let mut column_stats = ColumnStats {
            name: headers.get(column_index).unwrap_or("Unknown Column").to_string(),
            unique_count: unique_values.len(),
            missing_count,
            zero_count,
            one_count,
            total_rows,
            quality_score: 0.0,
            variability_percentage: 0.0,
            recommendation: String::new(),
        };

        column_stats.quality_score = calculate_quality_score(&column_stats);
        column_stats.variability_percentage = calculate_variability_percentage(&column_stats);
        column_stats.recommendation = get_recommendation(&column_stats);
        
        results.push(column_stats);
    }

    results.sort_by(|a, b| b.quality_score.partial_cmp(&a.quality_score).unwrap());

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
        "One Value Count",
        "Total Rows",
        "Missing %",
        "Zero %",
        "One %",
        "Valid %",
        "Variability %",  // Added new column
        "Recommendation"
    ])?;

    for stats in results {
        let missing_percentage = (stats.missing_count as f64 / stats.total_rows as f64 * 100.0).round();
        let zero_percentage = (stats.zero_count as f64 / stats.total_rows as f64 * 100.0).round();
        let one_percentage = (stats.one_count as f64 / stats.total_rows as f64 * 100.0).round();
        let valid_percentage = ((stats.total_rows - stats.missing_count - stats.zero_count - stats.one_count) as f64 
            / stats.total_rows as f64 * 100.0).round();

        writer.write_record(&[
            stats.name,
            format!("{:.1}", stats.quality_score),
            stats.unique_count.to_string(),
            stats.missing_count.to_string(),
            stats.zero_count.to_string(),
            stats.one_count.to_string(),
            stats.total_rows.to_string(),
            format!("{}%", missing_percentage),
            format!("{}%", zero_percentage),
            format!("{}%", one_percentage),
            format!("{}%", valid_percentage),
            format!("{:.1}%", stats.variability_percentage),  // Added variability percentage
            stats.recommendation,
        ])?;
    }

    writer.flush()?;
    println!("Results saved to {}", output_path);
    Ok(())
}

fn main() {
    let input_file_path = "/home/aricept094/mydata/PCO/sorted_columns_cleaned_output_good_targets.csv";
    let output_file_path = "/home/aricept094/mydata/PCO/analysis_results_all.csv";

    if !Path::new(input_file_path).exists() {
        println!("Error: Input file not found at {}", input_file_path);
        return;
    }

    if let Err(err) = analyze_csv(input_file_path, output_file_path) {
        println!("Error analyzing CSV: {}", err);
    }
}