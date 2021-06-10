use crate::framework::*;

use flate2::read::GzDecoder;
use glob::glob;

use std::any::Any;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::marker::PhantomData;
use std::path::PathBuf;

pub struct Glob {
    pub patterns: Vec<String>,
}

impl Transform for Glob {
    type Input = ();
    type Output = PathBuf;

    fn transform(&self, _input: Self::Input) -> Box<dyn Iterator<Item = Self::Output> + Send + '_> {
        let iter = self
            .patterns
            .clone()
            .into_iter()
            .flat_map(|pat| glob(&pat).expect("bad glob pattern"))
            .flat_map(|glob| match glob {
                Ok(path) => {
                    /*
                    let flowfile = AnyFlowFile {
                        data: Box::new(path),
                        source: "".to_string(),
                    };
                    */
                    Some(path)
                }
                Err(e) => {
                    dbg!(e);
                    None
                }
            });

        Box::new(iter)
    }
}

impl AnyTransform for Glob {
    fn transform_any(&self, _input: AnyFlowFile) -> Box<dyn Iterator<Item = AnyFlowFile> + Send> {
        let iter = self
            .patterns
            .clone()
            .into_iter()
            .flat_map(|pat| glob(&pat).expect("bad glob pattern"))
            .flat_map(|glob| match glob {
                Ok(path) => {
                    let flowfile = AnyFlowFile {
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
    type Input = PathBuf;
    type Output = Box<dyn Read + Send>;

    fn transform(&self, input: Self::Input) -> Box<dyn Iterator<Item = Self::Output> + Send> {
        let file = File::open(&input).unwrap();
        let reader = if matches!(input.to_str(), Some(p) if p.ends_with(".gz")) {
            Box::new(GzDecoder::new(file)) as Box<dyn Read + Send>
        } else {
            Box::new(file) as _
        };

        Box::new(std::iter::once(reader))
    }
}

impl AnyTransform for Unpack {
    fn transform_any(&self, input: AnyFlowFile) -> Box<dyn Iterator<Item = AnyFlowFile> + Send> {
        let path = input.data.downcast_ref::<PathBuf>().unwrap();
        let file = File::open(path).unwrap();
        let reader = if matches!(path.to_str(), Some(p) if p.ends_with(".gz")) {
            Box::new(GzDecoder::new(file)) as Box<dyn Any + Sync + Send + 'static>
        // as Box<dyn Read + Send>
        } else {
            Box::new(file) as _ // as Box<dyn Read + Send>
        };

        let flowfile = AnyFlowFile {
            data: reader,
            source: path.to_string_lossy().into(),
        };
        Box::new(std::iter::once(flowfile))
    }
}

pub struct Lines {}

impl Transform for Lines {
    type Input = Box<dyn Read + Send>;
    type Output = String;

    fn transform(&self, input: Self::Input) -> Box<dyn Iterator<Item = Self::Output> + Send> {
        Box::new(BufReader::new(input).lines().flat_map(Result::ok))
    }
}

impl AnyTransform for Lines {
    fn transform_any(&self, input: AnyFlowFile) -> Box<dyn Iterator<Item = AnyFlowFile> + Send> {
        let AnyFlowFile { data, source } = input;
        let file = data.downcast::<File>().unwrap();
        let mut count = 0;
        let iter = BufReader::new(file).lines().map(move |l| {
            count += 1;
            let mut source = source.clone();
            source.push_str(&format!(" {}", count));
            AnyFlowFile {
                data: Box::new(l),
                source,
            }
        });

        Box::new(iter)
    }
}

pub struct Nullify<A> {
    marker: PhantomData<A>,
}

impl<A> Default for Nullify<A> {
    fn default() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<A> Transform for Nullify<A> {
    type Input = A;
    type Output = ();

    fn transform(&self, _input: Self::Input) -> Box<dyn Iterator<Item = Self::Output> + Send> {
        Box::new(std::iter::once(()))
    }
}
