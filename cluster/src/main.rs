use calamine::{open_workbook, Reader, Xlsx};
use linfa::prelude::*;
use linfa_clustering::KMeans;
use ndarray::Array2;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    // Load the Excel file
    let path = "/home/aricept094/mydata/my_cluster.xlsx";
    let mut workbook: Xlsx<_> = open_workbook(path)?;

    // Read the data
    let range = workbook
        .worksheet_range("Sheet1")
        .ok_or("Cannot find 'Sheet1'")?
        .map_err(|e| Box::new(e) as Box<dyn Error>)?;

    // Collect the data into an array
    let mut data = Vec::new();
    for row in range.rows().skip(1) {
        let self_esteem_level = row[0].get_float().ok_or("Invalid data")?;
        let adhd_type = row[1].get_float().ok_or("Invalid data")?;
        data.push(self_esteem_level);
        data.push(adhd_type);
    }

    // Convert data to ndarray
    let data = Array2::from_shape_vec((data.len() / 2, 2), data)?;
    let dataset = DatasetBase::from(data.clone());

    // Define KMeans parameters and create model
    let n_clusters = 5;
    let model = KMeans::params(n_clusters)
        .max_n_iterations(100)
        .fit(&data)?;

    // Predict the clusters
    let predictions = model.predict(&data);

    // Print basic clustering results
    println!("Clustering completed with {} clusters", n_clusters);
    println!("Centroids:\n{}", model.centroids());

    Ok(())
}