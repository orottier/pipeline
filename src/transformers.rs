use crate::framework::*;

use flate2::{read::GzDecoder, write::GzEncoder};
use glob::glob;

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Read};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

pub struct Glob {
    pub patterns: Vec<String>,
}

impl StartTransform for Glob {
    type Output = PathBuf;
    type Iter = impl Iterator<Item = FlowFile<Self::Output>> + Send;

    fn start(&self) -> Self::Iter {
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

        iter
    }
}

pub struct Unpack {}

impl Transform for Unpack {
    type Input = PathBuf;
    type Output = Box<dyn Read + Send + Sync>;
    type Iter = impl Iterator<Item = FlowFile<Self::Output>> + Send;

    fn transform(&self, input: FlowFile<Self::Input>) -> Self::Iter {
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

        closeable
    }
}

pub struct Lines {}

impl Transform for Lines {
    type Input = Box<dyn Read + Send + Sync>;
    type Output = String;
    type Iter = impl Iterator<Item = FlowFile<Self::Output>> + Send;

    fn transform(&self, input: FlowFile<Self::Input>) -> Self::Iter {
        let FlowFile { data, source } = input;
        let iter = BufReader::new(data)
            .lines()
            .flat_map(Result::ok)
            .enumerate()
            .map(move |(i, l)| FlowFile {
                data: l,
                source: format!("{}:{}", source, i),
            });

        iter
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

impl<A> CloseTransform for Nullify<A> {
    type Input = A;

    fn close(&self, _input: FlowFile<Self::Input>) {
        // do nothing
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
    type Iter = impl Iterator<Item = FlowFile<Self::Output>> + Send;

    fn transform(&self, input: FlowFile<Self::Input>) -> Self::Iter {
        std::iter::once(input)
    }
}

pub struct Csv {}

impl Transform for Csv {
    type Input = Box<dyn Read + Send + Sync>;
    type Output = csv::StringRecord;
    type Iter = impl Iterator<Item = FlowFile<Self::Output>> + Send;

    fn transform(&self, input: FlowFile<Self::Input>) -> Self::Iter {
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
        iter
    }
}

pub struct Write {
    builder: Mutex<tar::Builder<GzEncoder<BufWriter<File>>>>,
}

impl Write {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path.as_ref())
            .unwrap();
        let buffered = BufWriter::new(file);
        let gz = GzEncoder::new(buffered, flate2::Compression::default());
        let tar = tar::Builder::new(gz);

        let builder = Mutex::new(tar);

        Self { builder }
    }
}

impl CloseTransform for Write {
    type Input = String;

    fn close(&self, input: FlowFile<Self::Input>) {
        let FlowFile { data, source } = input;
        let data = data.as_bytes();

        let mut header = tar::Header::new_gnu();
        header.set_size(data.len() as u64);
        header.set_cksum();

        let mut ar = self.builder.lock().unwrap();
        ar.append_data(&mut header, &source, data).unwrap();
    }
}

pub struct Contains<R> {
    needle: String,
    _marker: PhantomData<R>,
}

impl<R> Contains<R> {
    pub fn new<S: Into<String>>(needle: S) -> Self {
        Self {
            needle: needle.into(),
            _marker: PhantomData,
        }
    }
}

pub trait CheckContains {
    fn contains(&self, needle: &str) -> bool;
}

impl CheckContains for String {
    fn contains(&self, needle: &str) -> bool {
        self.as_str().contains(needle)
    }
}
impl CheckContains for Vec<u8> {
    fn contains(&self, needle: &str) -> bool {
        std::str::from_utf8(&self)
            .map(|s| s.contains(needle))
            .unwrap_or(false)
    }
}
impl CheckContains for csv::StringRecord {
    fn contains(&self, needle: &str) -> bool {
        self.iter().any(|i| i.contains(needle))
    }
}

impl<S: CheckContains + Send> Transform for Contains<S> {
    type Input = S;
    type Output = S;
    type Iter = impl Iterator<Item = FlowFile<Self::Output>> + Send;

    fn transform(&self, input: FlowFile<Self::Input>) -> Self::Iter {
        let contains = input.data.contains(&self.needle);
        std::iter::once(input).filter(move |_| contains)
    }
}
