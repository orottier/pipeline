use flate2::read::GzDecoder;
use glob::glob;
use rayon::iter::ParallelBridge;
use rayon::prelude::ParallelIterator;

use std::fs::File;
use std::io::Read;

fn main() {
    let a = glob("*.csv")
        .expect("bad glob pattern")
        .chain(glob("*.csv.gz").expect("bad glob pattern"))
        .flat_map(|glob| match glob {
            Ok(path) => Some(path),
            Err(e) => {
                dbg!(e);
                None
            }
        })
        .par_bridge()
        .map(|path| File::open(&path).map(|r| (r, path)))
        .flat_map(|io| match io {
            Ok((file, path)) => Some((file, path)),
            Err(e) => {
                dbg!(e);
                None
            }
        })
        .map(|(file, path)| {
            if matches!(path.to_str(), Some(p) if p.ends_with(".gz")) {
                Box::new(GzDecoder::new(file)) as Box<dyn Read + Send>
            } else {
                Box::new(file) as Box<dyn Read + Send>
            }
        })
        .flat_map(|file| {
            let rdr = csv::Reader::from_reader(file);
            rdr.into_records().par_bridge()
        })
        .flat_map(|csv| match csv {
            Ok(record) => Some(record),
            Err(e) => {
                dbg!(e);
                None
            }
        })
        .filter(|record| record.iter().any(|v| v.contains("1")))
        .count();

    dbg!(a);
}
