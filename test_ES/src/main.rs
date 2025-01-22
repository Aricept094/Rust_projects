use rand::prelude::*;
use rand::rngs::StdRng;
use rand_distr::{Normal, StandardNormal};
use rayon::prelude::*;
use std::sync::Arc;

struct Individual {
    params: Vec<f64>,
    sigmas: Vec<f64>,
    fitness: f64,
}

impl Individual {
    fn new(params: Vec<f64>, sigmas: Vec<f64>) -> Self {
        Individual {
            params,
            sigmas,
            fitness: f64::MAX,
        }
    }

    fn evaluate(&mut self, features: &[Vec<f64>], targets: &[f64]) {
        let mut loss = 0.0;
        for (x, y) in features.iter().zip(targets) {
            let logit = self.params[..x.len()].iter().zip(x.iter())
                .map(|(w, xi)| w * xi)
                .sum::<f64>() + self.params.last().unwrap();
            
            let prob = 1.0 / (1.0 + (-logit).exp()).clamp(1e-15, 1.0 - 1e-15);
            loss += - (y * prob.ln() + (1.0 - y) * (1.0 - prob).ln());
        }
        
        // Reduced regularization strength
        let l2: f64 = self.params.iter().map(|w| w.powi(2)).sum();
        let l1: f64 = self.params.iter().map(|w| w.abs()).sum();
        loss += 1.0 * l2 + 0.5 * l1;  // Reduced regularization coefficients
        
        self.fitness = loss / features.len() as f64;
    }

    fn predict(&self, features: &[Vec<f64>]) -> Vec<f64> {
        features.iter().map(|x| {
            let logit = self.params[..x.len()].iter().zip(x.iter())
                .map(|(w, xi)| w * xi)
                .sum::<f64>() + self.params.last().unwrap();
            1.0 / (1.0 + (-logit).exp())
        }).collect()
    }
}

fn main() {
    let (train_features, train_targets) = generate_data(1000);
    let (test_features, test_targets) = generate_data(200);
    
    let features = Arc::new(train_features);
    let targets = Arc::new(train_targets);
    let param_count = features[0].len() + 1;

    // ES configuration
    let mu = 50;
    let lambda = 200;
    let generations = 1000;
    let rho = 15;
    let tau = 0.1;

    let population: Vec<Individual> = (0..mu)
        .into_par_iter()
        .map_init(|| StdRng::from_entropy(), |rng, _| {
            let params = (0..param_count).map(|_| rng.gen_range(-1.0..1.0)).collect();
            let sigmas = vec![0.2; param_count];
            let mut ind = Individual::new(params, sigmas);
            ind.evaluate(&features, &targets);
            ind
        })
        .collect();

    let (population, _) = (0..generations).fold((population, 0), |(pop, _), gen| {
        let offspring: Vec<Individual> = (0..lambda)
            .into_par_iter()
            .map_init(|| StdRng::from_entropy(), |rng, _| {
                let mut candidates = pop.iter().collect::<Vec<_>>();
                candidates.sort_by(|a, b| a.fitness.partial_cmp(&b.fitness).unwrap());
                let parents = &candidates[..rho];

                let alpha = rng.gen_range(0.4..0.6);
                let mut child_params = vec![0.0; param_count];
                let mut child_sigmas = vec![0.0; param_count];
                
                for i in 0..param_count {
                    let base = parents[0].params[i];
                    
                    // Fixed range calculation with proper initial values
                    let max_val = parents.iter()
                        .map(|p| p.params[i])
                        .fold(f64::NEG_INFINITY, |a, b| a.max(b));
                    let min_val = parents.iter()
                        .map(|p| p.params[i])
                        .fold(f64::INFINITY, |a, b| a.min(b));
                    let range = max_val - min_val;
                    
                    child_params[i] = base + alpha * range * rng.gen_range(-0.5..0.5);
                    child_sigmas[i] = parents.iter()
                        .map(|p| p.sigmas[i])
                        .sum::<f64>() / rho as f64;
                }

                for i in 0..param_count {
                    child_sigmas[i] *= (tau * rng.sample::<f64, _>(StandardNormal))
                        .exp()
                        .max(0.5)
                        .min(2.0);
                    child_sigmas[i] = child_sigmas[i].clamp(1e-3, 0.5);
                    
                    let normal = Normal::new(0.0, child_sigmas[i]).unwrap();
                    child_params[i] += normal.sample(rng);
                    child_params[i] = child_params[i].clamp(-3.0, 3.0);
                }

                let mut ind = Individual::new(child_params, child_sigmas);
                ind.evaluate(&features, &targets);
                ind
            })
            .collect();

        let mut new_pop = pop.into_par_iter().chain(offspring).collect::<Vec<_>>();
        new_pop.par_sort_unstable_by(|a, b| a.fitness.partial_cmp(&b.fitness).unwrap());
        new_pop.truncate(mu);

        if gen % 50 == 0 {
            let avg_sigma = new_pop[0].sigmas.iter().sum::<f64>() / param_count as f64;
            println!("Gen {:03} | Loss: {:.4} | Sigma: {:.3e} | Weights: {:.2?}",
                gen, new_pop[0].fitness, avg_sigma,
                &new_pop[0].params[..param_count-1]
            );
        }

        (new_pop, gen)
    });

    let best = &population[0];
    let predictions = best.predict(&test_features);
    let accuracy = predictions.iter().zip(test_targets.iter())
        .map(|(p, y)| ((p >= &0.5) == (y == &1.0)) as u32)
        .sum::<u32>() as f64 / test_targets.len() as f64;

    println!("\nTest Results:");
    println!("Accuracy: {:.2}%", accuracy * 100.0);
    println!("Weights: {:.2?}", &best.params[..param_count-1]);
    println!("Bias: {:.4}", best.params[param_count-1]);
}

fn generate_data(n: usize) -> (Vec<Vec<f64>>, Vec<f64>) {
    let mut rng = StdRng::from_entropy();
    let mut features = Vec::with_capacity(n);
    let mut targets = Vec::with_capacity(n);
    
    let true_weights = vec![0.5, 1.5];
    let true_bias = -0.3;
    
    for _ in 0..n {
        let x1 = rng.gen_range(-2.0..2.0);
        let x2 = rng.gen_range(-2.0..2.0);
        let noise = rng.sample::<f64, _>(StandardNormal) * 0.5;  // Gaussian noise
        
        // Correct data generation: additive noise to linear combination
        let z = true_weights[0] * x1 + true_weights[1] * x2 + true_bias + noise;
        let y = if z > 0.0 { 1.0 } else { 0.0 };
        
        features.push(vec![x1, x2]);
        targets.push(y);
    }
    
    (features, targets)
}