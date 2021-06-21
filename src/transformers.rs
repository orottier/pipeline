use crate::framework::*;

use flate2::{read::GzDecoder, write::GzEncoder};
use glob::glob;

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Read};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

pub struct Glob {
    patterns: Vec<String>,
}

impl StartTransform for Glob {
    type Output = PathBuf;
    type Iter = impl Iterator<Item = FlowFile<Self::Output>> + Send;

    fn start(self) -> Self::Iter {
        self.patterns
            .into_iter()
            .flat_map(|pat| glob(&pat).expect("bad glob pattern"))
            .flat_map(|glob| match glob {
                Ok(path) => Some(path),
                Err(e) => {
                    log::error!("Exception in Glob {:?}", e);
                    None
                }
            })
            .map(FlowFile::new)
    }
}

impl From<Vec<String>> for Glob {
    fn from(args: Vec<String>) -> Self {
        Self { patterns: args }
    }
}

pub struct Unpack {}

impl From<Vec<String>> for Unpack {
    fn from(_args: Vec<String>) -> Self {
        Self {}
    }
}

impl Transform for Unpack {
    type Input = PathBuf;
    type Output = Box<dyn Read + Send + Sync>;
    type Iter = impl Iterator<Item = FlowFile<Self::Output>> + Send;

    fn transform(&self, input: FlowFile<Self::Input>) -> Self::Iter {
        let FlowFile { data, mut meta } = input;

        let file = File::open(&data).unwrap();
        log::debug!("now processing {}", &data.to_string_lossy());

        let reader = if matches!(data.to_str(), Some(p) if p.ends_with(".gz")) {
            Box::new(GzDecoder::new(file)) as Box<dyn Read + Send + Sync>
        } else {
            Box::new(file) as _
        };

        // set file path as source
        meta.add_source(&data.to_string_lossy());

        let flow_file = FlowFile { data: reader, meta };
        let iter = std::iter::once(flow_file);

        let data_clone = data.clone();
        CloseableIter::new(
            iter,
            move || log::debug!("processing success {:?}", data_clone),
            move || log::debug!("processing failure {:?}", data),
        )
    }
}

pub struct Lines {}

impl From<Vec<String>> for Lines {
    fn from(_args: Vec<String>) -> Self {
        Self {}
    }
}

impl Transform for Lines {
    type Input = Box<dyn Read + Send + Sync>;
    type Output = String;
    type Iter = impl Iterator<Item = FlowFile<Self::Output>> + Send;

    fn transform(&self, input: FlowFile<Self::Input>) -> Self::Iter {
        let FlowFile { data, meta } = input;

        BufReader::new(data)
            .lines()
            .enumerate()
            .flat_map(move |(i, lr)| match lr {
                Ok(l) => {
                    let mut my_meta = meta.clone();
                    my_meta.add_source(&format!(":{}", i));

                    Some(FlowFile {
                        data: l,
                        meta: my_meta,
                    })
                }
                Err(e) => {
                    log::error!("Exception in Lines: {:?}", e);
                    meta.mark_failed();
                    None
                }
            })
    }
}

pub struct Nullify<A> {
    marker: PhantomData<A>,
}

impl<A> From<Vec<String>> for Nullify<A> {
    fn from(_args: Vec<String>) -> Self {
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

impl<A> From<Vec<String>> for Identity<A> {
    fn from(_args: Vec<String>) -> Self {
        Self::default()
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

pub struct ToString<A> {
    marker: PhantomData<A>,
}

impl<A> Default for ToString<A> {
    fn default() -> Self {
        Self {
            marker: PhantomData,
        }
    }
}

impl<A> From<Vec<String>> for ToString<A> {
    fn from(_args: Vec<String>) -> Self {
        Self::default()
    }
}

impl<A: Send + Sync + std::fmt::Debug + 'static> Transform for ToString<A> {
    type Input = A;
    type Output = String;
    type Iter = impl Iterator<Item = FlowFile<Self::Output>> + Send;

    fn transform(&self, input: FlowFile<Self::Input>) -> Self::Iter {
        let FlowFile { data, meta } = input;
        let data = format!("{:?}", data);
        std::iter::once(FlowFile { data, meta })
    }
}

pub struct Csv {}

impl From<Vec<String>> for Csv {
    fn from(_args: Vec<String>) -> Self {
        Self {}
    }
}

impl Transform for Csv {
    type Input = Box<dyn Read + Send + Sync>;
    type Output = csv::StringRecord;
    type Iter = impl Iterator<Item = FlowFile<Self::Output>> + Send;

    fn transform(&self, input: FlowFile<Self::Input>) -> Self::Iter {
        let FlowFile { data, meta } = input;

        csv::Reader::from_reader(data)
            .into_records()
            .enumerate()
            .flat_map(move |(i, r)| match r {
                Ok(v) => {
                    let mut my_meta = meta.clone();
                    my_meta.add_source(&format!(":{}", i));

                    Some(FlowFile {
                        data: v,
                        meta: my_meta,
                    })
                }
                Err(e) => {
                    log::error!("Exception in Csv: {:?}", e);
                    meta.mark_failed();
                    None
                }
            })
    }
}

pub struct Write {
    builder: Mutex<tar::Builder<GzEncoder<BufWriter<File>>>>,
}

impl From<Vec<String>> for Write {
    fn from(args: Vec<String>) -> Self {
        Self::new(&args[0])
    }
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
        let FlowFile { data, meta } = input;
        let data = data.as_bytes();

        let mut header = tar::Header::new_gnu();
        header.set_size(data.len() as u64);
        header.set_cksum();

        let mut ar = self.builder.lock().unwrap();
        ar.append_data(&mut header, meta.source(), data).unwrap();
    }
}

pub struct Contains<R> {
    needle: String,
    _marker: PhantomData<R>,
}

impl<R> From<Vec<String>> for Contains<R> {
    fn from(mut args: Vec<String>) -> Self {
        Self {
            needle: args.remove(0),
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
