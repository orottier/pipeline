use crate::framework::{FlowFile, Transform};

use flate2::read::GzDecoder;
use glob::glob;

use std::any::Any;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

pub struct Glob {
    pub patterns: Vec<String>,
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

pub struct Unpack {}

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

pub struct Lines {}

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
