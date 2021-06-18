use rayon_ingest::framework::*;
use rayon_ingest::transformers::*;

use rayon::iter::ParallelBridge;
use rayon::prelude::ParallelIterator;

fn main() {
    let g = Glob {
        patterns: vec!["open*.csv".to_string()],
    };
    let u = Unpack {};
    let l = Lines {};
    /*
    let c = Csv {};
    */
    let f = Contains::new("1");
    let w = Write::new("output.tar.gz");

    let r = g
        .transform(FlowFile::genesis())
        .par_bridge()
        .flat_map(|i| u.transform(i).par_bridge())
        .flat_map(|i| l.transform(i).par_bridge())
        .flat_map(|i| f.transform(i).par_bridge())
        .flat_map(|i| w.transform(i).par_bridge())
        .count();

    dbg!(r);
}
