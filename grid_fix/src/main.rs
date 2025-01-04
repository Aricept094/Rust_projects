use std::error::Error;
use csv::{ReaderBuilder, WriterBuilder};

fn main() -> Result<(), Box<dyn Error>> {
    // Path to your original 32x256 CSV data (no headers).
    let input_file_path = "/home/aricept094/mydata/testrm.csv";
    
    // Path to the output CSV file.
    let output_file_path = "/home/aricept094/mydata/testrm_converted.csv";

    // Set the known dimensions of your dataset
    let num_meridians = 32;
    let num_radials = 256;

    // 1. Create a CSV reader, specifying that there are no headers in the file.
    let mut rdr = ReaderBuilder::new()
        .has_headers(false)
        .from_path(input_file_path)?;

    // 2. Create a CSV writer to output our transformed data.
    let mut wtr = WriterBuilder::new()
        .has_headers(false) // We'll manually write the header
        .from_path(output_file_path)?;

    // 3. Write the header row we want in the output CSV, including Keratometry_Value.
    wtr.write_record(&[
        "Meridian_Index",
        "Radial_Index",
        "Meridian_Angle_Deg",
        "Meridian_Angle_Rad",
        "Normalized_Radius",
        "Cos_Theta",
        "Sin_Theta",
        "X_Coordinate",
        "Y_Coordinate",
        "Keratometry_Value",  // New column
    ])?;

    // We'll iterate over the rows, each row corresponds to one radial index.
    // `radial_index_1_based` will go from 1..=256
    let mut radial_index_1_based = 0;

    for result in rdr.records() {
        radial_index_1_based += 1;

        // Parse the current row (32 columns)
        let record = result?;

        // Each column in the row corresponds to a meridian index from 1..=32
        for (meridian_index, value_str) in record.iter().enumerate() {
            // Parse the keratometry reading as a float
            let k_reading: f64 = value_str.parse()?;

            let meridian_index_1_based = meridian_index + 1;
            
            // Compute Meridian_Angle_Deg (0..360)
            let meridian_angle_deg = (meridian_index_1_based as f64 - 1.0)
                * (360.0 / num_meridians as f64);
            
            // Convert degrees to radians
            let meridian_angle_rad = meridian_angle_deg.to_radians();
            
            // Compute normalized radius in [0, 1].
            let normalized_radius = (radial_index_1_based as f64 - 1.0)
                / (num_radials as f64 - 1.0);
            
            // Compute cos and sin for the angle
            let cos_theta = meridian_angle_rad.cos();
            let sin_theta = meridian_angle_rad.sin();
            
            // Compute Cartesian coordinates
            let x_coordinate = normalized_radius * cos_theta;
            let y_coordinate = normalized_radius * sin_theta;
            
            // Write the transformed record to the output CSV, including keratometry
            wtr.write_record(&[
                meridian_index_1_based.to_string(),
                radial_index_1_based.to_string(),
                meridian_angle_deg.to_string(),
                meridian_angle_rad.to_string(),
                normalized_radius.to_string(),
                cos_theta.to_string(),
                sin_theta.to_string(),
                x_coordinate.to_string(),
                y_coordinate.to_string(),
                k_reading.to_string(),  // New column
            ])?;
        }
    }

    // 4. Flush the writer to make sure all data is written to disk.
    wtr.flush()?;

    println!("Data successfully converted (with Keratometry_Value) and saved to {}", output_file_path);
    Ok(())
}
