use serde::Deserialize;
use std::env;
use std::fs::File;
use std::io::Read;
use std::path::Path;

#[derive(Deserialize)]
struct PatientData {
    question2: i32, // Age
    #[serde(rename = "question4")]
    question4: Option<String>, // Current smoker?
    #[serde(rename = "question28")]
    question28: Option<String>, // Former smoker?
    #[serde(rename = "question30")]
    question30: Option<f64>, // How many years did you smoke?
    #[serde(rename = "question29")]
    question29: Option<f64>, // How many years since you quit smoking?
}

fn is_indicated_for_lung_cancer_screening(patient_data: &PatientData) -> bool {
    // Extract relevant data
    let age = patient_data.question2;
    let currently_smokes = patient_data.question4.as_deref() == Some("Item 2");
    let previously_smoked = patient_data.question28.as_deref() == Some("Item 2");
    let years_smoked = patient_data.question30.unwrap_or(0.0);
    let years_since_quit = patient_data.question29.unwrap_or(0.0);

    // Apply the screening criteria
    if age >= 50 && age <= 80 {
        if currently_smokes || (previously_smoked && years_since_quit <= 15.0) {
            let pack_years = years_smoked; // Assuming 1 pack per day
            if pack_years >= 20.0 {
                return true;
            }
        }
    }

    false
}

fn read_json_from_file<P: AsRef<Path>>(path: P) -> Result<PatientData, Box<dyn std::error::Error>> {
    let mut file = File::open(path)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    let data: PatientData = serde_json::from_str(&contents)?;
    Ok(data)
}

fn main() {
    // Get the file path from command-line arguments
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <path_to_json_file>", args[0]);
        std::process::exit(1);
    }
    let file_path = &args[1];

    match read_json_from_file(file_path) {
        Ok(patient_data) => {
            if is_indicated_for_lung_cancer_screening(&patient_data) {
                println!("Patient is indicated for lung cancer screening.");
            } else {
                println!("Patient is not indicated for lung cancer screening.");
            }
        }
        Err(err) => {
            eprintln!("Error reading or parsing JSON file: {}", err);
            std::process::exit(1);
        }
    }
}