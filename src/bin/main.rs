use rayon_ingest::framework::*;
use rayon_ingest::transformers::*;

use rayon::iter::ParallelBridge;
use rayon::prelude::ParallelIterator;

use env_logger::Env;

fn main() {
    // setup logger, DEBUG level by default
    env_logger::Builder::from_env(Env::default().default_filter_or("debug")).init();

    let g = Glob::from(vec!["testcase.csv".to_string()]);
    let u = Unpack {};
    let t = CsvInnerJoin {};
    let s = ToString::from(vec![]);
    let o = StdOut {};

    let stats = Stats::new();

    g.start()
        .par_bridge()
        .flat_map(|i| u.transform(i).par_bridge())
        .flat_map(|i| t.transform(i).par_bridge())
        .flat_map(|i| s.transform(i).par_bridge())
        .for_each(|i| {
            o.close(i);
            stats.increment();
        });
}
