use flate2::read::GzDecoder;
use glob::glob;

use std::any::Any;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

pub struct FlowFile {
    data: Box<dyn Any + Sync + Send + 'static>,
    source: String,
}

pub trait Transform {
    fn transform(&self, input: FlowFile) -> Box<dyn Iterator<Item = FlowFile> + Send + '_>;
}

struct ChainAny {
    first: Box<dyn Transform + Send + Sync + 'static>,
    next: Box<dyn Transform + Send + Sync + 'static>,
}

impl Transform for ChainAny {
    fn transform(&self, input: FlowFile) -> Box<dyn Iterator<Item = FlowFile> + Send + '_> {
        let iter = self
            .first
            .transform(input)
            .flat_map(move |o| self.next.transform(o));
        Box::new(iter)
    }
}

struct Glob {
    patterns: Vec<String>,
}

impl Transform for Glob {
    fn transform(&self, _input: FlowFile) -> Box<dyn Iterator<Item = FlowFile> + Send> {
        let iter = self
            .patterns
            .clone()
            .into_iter()
            .flat_map(|pat| glob(&pat).expect("bad glob pattern"))
            .flat_map(|glob| match glob {
                Ok(path) => {
                    let flowfile = FlowFile {
                        data: Box::new(path),
                        source: "".to_string(),
                    };
                    Some(flowfile)
                }
                Err(e) => {
                    dbg!(e);
                    None
                }
            });

        Box::new(iter)
    }
}

struct Unpack {}

impl Transform for Unpack {
    fn transform(&self, input: FlowFile) -> Box<dyn Iterator<Item = FlowFile> + Send> {
        let path = input.data.downcast_ref::<PathBuf>().unwrap();
        let file = File::open(path).unwrap();
        let reader = if matches!(path.to_str(), Some(p) if p.ends_with(".gz")) {
            Box::new(GzDecoder::new(file)) as Box<dyn Any + Sync + Send + 'static>
        // as Box<dyn Read + Send>
        } else {
            Box::new(file) as _ // as Box<dyn Read + Send>
        };

        let flowfile = FlowFile {
            data: reader,
            source: path.to_string_lossy().into(),
        };
        Box::new(std::iter::once(flowfile))
    }
}

struct Lines {}

impl Transform for Lines {
    fn transform(&self, input: FlowFile) -> Box<dyn Iterator<Item = FlowFile> + Send> {
        let FlowFile { data, source } = input;
        let file = data.downcast::<File>().unwrap();
        let mut count = 0;
        let iter = BufReader::new(file).lines().map(move |l| {
            count += 1;
            let mut source = source.clone();
            source.push_str(&format!(" {}", count));
            FlowFile {
                data: Box::new(l),
                source,
            }
        });

        Box::new(iter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rayon::iter::ParallelBridge;
    use rayon::prelude::ParallelIterator;

    #[test]
    fn it_works() {
        let g = Glob {
            patterns: vec!["*.toml".into()],
        };
        let u = Unpack {};
        let l = Lines {};

        let genesis = FlowFile {
            data: Box::new(()),
            source: "".into(),
        };
        let a = g
            .transform(genesis)
            .par_bridge()
            .flat_map(|i| u.transform(i).par_bridge())
            .flat_map(|i| l.transform(i).par_bridge())
            .count();

        assert_eq!(a, 12);
    }

    #[test]
    fn chain_any() {
        let g = Glob {
            patterns: vec!["*.toml".into()],
        };
        let u = Unpack {};
        let l = Lines {};

        let genesis = FlowFile {
            data: Box::new(()),
            source: "".into(),
        };
        let mut a: ChainAny = ChainAny {
            first: Box::new(g),
            next: Box::new(u),
        };
        a = ChainAny {
            first: Box::new(a),
            next: Box::new(l),
        };

        let c = a.transform(genesis).count();
        assert_eq!(c, 12);
    }
}
