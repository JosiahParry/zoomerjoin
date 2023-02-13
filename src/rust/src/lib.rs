use extendr_api::prelude::parallel::prelude::ParallelIterator;
use extendr_api::prelude::*;

use rayon::prelude::*;

pub mod shingleset;
use crate::shingleset::ShingleSet;

pub mod minihasher;

pub mod lshjoiner;
use crate::lshjoiner::LSHjoiner;

#[extendr]
fn rust_jaccard_similarity(left_string_r: Robj, right_string_r: Robj, ngram_width: i64) -> Doubles {
    let left_string_vec = <Vec<String>>::from_robj(&left_string_r).unwrap();
    let right_string_vec = <Vec<String>>::from_robj(&right_string_r).unwrap();

    // vector to hold sets of n_gram strings in each document
    let left_set_vec: Vec<ShingleSet> = left_string_vec
        .par_iter()
        .enumerate()
        .map(|(i, x)| ShingleSet::new(x, ngram_width as usize, i, None))
        .collect();
    let right_set_vec: Vec<ShingleSet> = right_string_vec
        .par_iter()
        .enumerate()
        .map(|(i, x)| ShingleSet::new(x, ngram_width as usize, i, None))
        .collect();

    let out_vec = left_set_vec
        .into_par_iter()
        .zip(right_set_vec)
        .map(|(a, b)| a.jaccard_similarity(&b))
        .collect::<Vec<f64>>();

    out_vec
        .into_iter()
        .map(|i| Rfloat::from(i))
        .collect::<Doubles>()
}

#[extendr]
fn rust_lsh_join(
    left_string_r: Robj,
    right_string_r: Robj,
    ngram_width: i64,
    n_bands: i64,
    band_size: i64,
    threshold: f64,
) -> Robj {
    let left_string_vec = <Vec<String>>::from_robj(&left_string_r).unwrap();
    let right_string_vec = <Vec<String>>::from_robj(&right_string_r).unwrap();

    let joiner = LSHjoiner::new(left_string_vec, right_string_vec, ngram_width as usize);

    let chosen_indexes = joiner.join(n_bands as usize, band_size as usize, threshold);

    let mut out_arr: Array2<u64> = Array2::zeros((chosen_indexes.len(), 2));
    for (i, pair) in chosen_indexes.iter().enumerate() {
        out_arr[[i, 0]] = pair.1 as u64 + 1;
        out_arr[[i, 1]] = pair.0 as u64 + 1;
    }

    Robj::try_from(&out_arr).into()
}

#[extendr]
fn rust_salted_lsh_join(
    left_string_r: Robj,
    right_string_r: Robj,
    left_salt_r: Robj,
    right_salt_r: Robj,
    ngram_width: i64,
    n_bands: i64,
    band_size: i64,
    threshold: f64,
) -> Robj {
    let left_string_vec = <Vec<String>>::from_robj(&left_string_r).unwrap();
    let right_string_vec = <Vec<String>>::from_robj(&right_string_r).unwrap();

    let left_salt_vec = <Vec<String>>::from_robj(&left_salt_r).unwrap();
    let right_salt_vec = <Vec<String>>::from_robj(&right_salt_r).unwrap();

    let joiner = LSHjoiner::new_with_salt(
        left_string_vec,
        right_string_vec,
        left_salt_vec,
        right_salt_vec,
        ngram_width as usize,
    );

    let chosen_indexes = joiner.join(n_bands as usize, band_size as usize, threshold);

    let mut out_arr: Array2<u64> = Array2::zeros((chosen_indexes.len(), 2));
    for (i, pair) in chosen_indexes.iter().enumerate() {
        out_arr[[i, 0]] = pair.1 as u64 + 1;
        out_arr[[i, 1]] = pair.0 as u64 + 1;
    }

    Robj::try_from(&out_arr).into()
}

// Macro to generate exports.
// This ensures exported functions are registered with R.
// See corresponding C code in `entrypoint.c`.
extendr_module! {
    mod zoomerjoin;
    fn rust_lsh_join;
    fn rust_salted_lsh_join;
    fn rust_jaccard_similarity;
    // fn em_link;
}
