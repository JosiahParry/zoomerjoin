use std::collections::HashSet;

use std::hash::{Hash, Hasher};

use fxhash::FxHasher;

#[derive(Debug)]
pub struct ShingleSet {
    pub shingles: HashSet<u64>,
    pub shingle_len : usize,
}

impl ShingleSet {
    pub fn new(string: &String, shingle_len: usize) -> Self {
        let mut out_set: HashSet<u64> = HashSet::new();

        let char_vec: Vec<char> = string.chars().collect();

        for window in char_vec.windows(shingle_len) {
            let mut hasher = FxHasher::default();

            window.hash(&mut hasher);

            let result: u64 = hasher.finish();

            out_set.insert(result);
        }


        ShingleSet { shingles: out_set , shingle_len }
    }

    pub fn jaccard_similarity(&self, b: &Self) -> f64 {
        // println!("{:?}",self.shingles.intersection(&b.shingles));
        // println!("{:?}", self.shingles.union(&b.shingles));

        self.shingles.intersection(&b.shingles).count() as f64
            / self.shingles.union(&b.shingles).count() as f64
    }
}