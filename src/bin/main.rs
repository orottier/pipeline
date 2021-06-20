use rayon_ingest::framework::*;
use rayon_ingest::transformers::*;

use rayon::iter::ParallelBridge;
use rayon::prelude::ParallelIterator;

use env_logger::Env;

fn main() {
    // setup logger, DEBUG level by default
    env_logger::Builder::from_env(Env::default().default_filter_or("debug")).init();

    let g = Glob::from(vec!["*.csv.gz".to_string()]);
    let u = Unpack {};
    //let l = Lines {};
    let c = Csv {};
    //let f = Contains::new("1");
    //let w = Write::new("output.tar.gz");
    let n = Nullify::from(vec![]);

    let stats = Stats::new();

    g.start()
        .par_bridge()
        .flat_map(|i| u.transform(i).par_bridge())
        .flat_map(|i| c.transform(i).par_bridge())
        //.flat_map(|i| f.transform(i).par_bridge())
        .for_each(|i| {
            n.close(i);
            stats.increment();
        });
}
