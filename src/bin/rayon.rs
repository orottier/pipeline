use rayon_ingest::framework::*;
use rayon_ingest::transformers::*;

use rayon::iter::ParallelBridge;
use rayon::prelude::ParallelIterator;

fn main() {
    let g = Glob {
        patterns: vec!["*.csv".to_string(), "*.csv.gz".to_string()],
    };
    let u = Unpack {};
    let c = Csv {};

    let r = g
        .transform(FlowFile::genesis())
        .par_bridge()
        .flat_map(|i| u.transform(i).par_bridge())
        .flat_map(|i| c.transform(i).par_bridge())
        .count();

    dbg!(r);
}
