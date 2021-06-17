use crate::framework::*;

use flate2::read::GzDecoder;
use glob::glob;

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

    fn transform(
        &self,
        _input: FlowFile<Self::Input>,
    ) -> Box<dyn Iterator<Item = FlowFile<Self::Output>> + Send + '_> {
        let iter = self
            .patterns
            .clone()
            .into_iter()
            .flat_map(|pat| glob(&pat).expect("bad glob pattern"))
            .flat_map(|glob| match glob {
                Ok(path) => Some(path),
                Err(e) => {
                    dbg!(e);
                    None
                }
            })
            .map(|path| FlowFile {
                data: path,
                source: String::new(),
            });

        Box::new(iter)
    }
}

pub struct Unpack {}

impl Transform for Unpack {
    type Input = PathBuf;
    type Output = Box<dyn Read + Send + Sync>;

    fn transform(
        &self,
        input: FlowFile<Self::Input>,
    ) -> Box<dyn Iterator<Item = FlowFile<Self::Output>> + Send> {
        let input = input.data; // unwrap flowfile

        let file = File::open(&input).unwrap();
        let reader = if matches!(input.to_str(), Some(p) if p.ends_with(".gz")) {
            Box::new(GzDecoder::new(file)) as Box<dyn Read + Send + Sync>
        } else {
            Box::new(file) as _
        };

        let flow_file = FlowFile {
            data: reader,
            source: input.to_string_lossy().into(),
        };
        let iter = std::iter::once(flow_file);

        let closeable = CloseableIter::new(iter, move || println!("done processing {:?}", input));

        Box::new(closeable)
    }
}

pub struct Lines {}

impl Transform for Lines {
    type Input = Box<dyn Read + Send + Sync>;
    type Output = String;

    fn transform(
        &self,
        input: FlowFile<Self::Input>,
    ) -> Box<dyn Iterator<Item = FlowFile<Self::Output>> + Send> {
        let FlowFile { data, source } = input;
        let iter = BufReader::new(data)
            .lines()
            .flat_map(Result::ok)
            .enumerate()
            .map(move |(i, l)| FlowFile {
                data: l,
                source: format!("{}:{}", source, i),
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

    fn transform(
        &self,
        input: FlowFile<Self::Input>,
    ) -> Box<dyn Iterator<Item = FlowFile<Self::Output>> + Send> {
        let FlowFile { data: _, source } = input;
        Box::new(std::iter::once(FlowFile { data: (), source }))
    }
}

pub struct Identity<A> {
    marker: PhantomData<A>,
}

impl<A> Default for Identity<A> {
    fn default() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<A: Send + Sync + 'static> Transform for Identity<A> {
    type Input = A;
    type Output = A;

    fn transform(
        &self,
        input: FlowFile<Self::Input>,
    ) -> Box<dyn Iterator<Item = FlowFile<Self::Output>> + Send> {
        Box::new(std::iter::once(input))
    }
}

pub struct Csv {}

impl Transform for Csv {
    type Input = Box<dyn Read + Send + Sync>;
    type Output = csv::StringRecord;

    fn transform(
        &self,
        input: FlowFile<Self::Input>,
    ) -> Box<dyn Iterator<Item = FlowFile<Self::Output>> + Send> {
        let FlowFile { data, source } = input;
        let iter = csv::Reader::from_reader(data)
            .into_records()
            .flat_map(|r| match r {
                Ok(v) => Some(v),
                Err(e) => {
                    dbg!(e);
                    None
                }
            })
            .enumerate()
            .map(move |(i, r)| FlowFile {
                data: r,
                source: format!("{}:{}", source, i),
            });
        Box::new(iter)
    }
}
