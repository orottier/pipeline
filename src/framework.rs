use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

#[derive(Debug)]
pub struct FlowFile<T> {
    pub data: T,
    pub meta: FlowFileMeta,
}

impl<T> FlowFile<T> {
    pub fn new(data: T) -> Self {
        Self {
            data,
            meta: FlowFileMeta::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct FlowFileMeta {
    source: String,
    failed: Option<&'static AtomicBool>,
}

impl FlowFileMeta {
    fn new() -> Self {
        FlowFileMeta {
            source: String::new(),
            failed: None,
        }
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn add_source(&mut self, s: &str) {
        self.source.push_str(s)
    }

    pub fn mark_failed(&self) {
        if let Some(failed) = &self.failed {
            failed.store(true, Ordering::SeqCst);
        }
    }
}

#[derive(Clone)]
pub struct Stats {
    total: Arc<AtomicU64>,
    start: SystemTime,
}

impl Stats {
    pub fn new() -> Self {
        let me = Self {
            total: Arc::new(AtomicU64::new(0)),
            start: SystemTime::now(),
        };
        let clone = me.clone();
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_secs(1));
            clone.report();
        });

        me
    }

    pub fn increment(&self) {
        self.total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn total(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }

    pub fn elapsed(&self) -> Duration {
        SystemTime::now().duration_since(self.start).unwrap()
    }

    fn report(&self) {
        let total = self.total();
        let elapsed = self.elapsed();
        let millis = elapsed.as_millis() as u64;
        let rate = if millis > 0 { total * 1000 / millis } else { 0 };
        log::info!("Processed {:10} items at {:10} msgs/sec", total, rate);
    }
}

impl Drop for Stats {
    fn drop(&mut self) {
        self.report()
    }
}

pub trait Transform: From<Vec<String>> {
    type Input;
    type Output;
    type Iter: Iterator<Item = FlowFile<Self::Output>> + Send;

    fn transform(&self, input: FlowFile<Self::Input>) -> Self::Iter;
}

pub trait StartTransform: From<Vec<String>> {
    type Output;
    type Iter: Iterator<Item = FlowFile<Self::Output>> + Send;

    fn start(self) -> Self::Iter;
}

pub trait CloseTransform: From<Vec<String>> {
    type Input;

    fn close(&self, input: FlowFile<Self::Input>);
}

pub struct CloseableIter<R, I: Iterator<Item = FlowFile<R>>, F1: Fn(), F2: Fn()> {
    iter: I,
    has_failed: &'static AtomicBool,
    on_success: F1,
    on_failure: F2,
}

impl<R, I: Iterator<Item = FlowFile<R>>, F1: Fn(), F2: Fn()> CloseableIter<R, I, F1, F2> {
    pub fn new(iter: I, on_success: F1, on_failure: F2) -> Self {
        let failed = AtomicBool::new(false);

        // For performance reasons, put the bool on the stack and leak it (make it 'static').
        // Then we don't need to refcount it all the way down the pipeline (via Arc).
        let has_failed = Box::leak(Box::new(failed));

        Self {
            iter,
            has_failed,
            on_success,
            on_failure,
        }
    }
}

impl<R, I: Iterator<Item = FlowFile<R>>, F1: Fn(), F2: Fn()> Iterator
    for CloseableIter<R, I, F1, F2>
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|mut f| {
            if f.meta.failed.is_none() {
                f.meta.failed = Some(self.has_failed);
            }
            f
        })
    }
}

impl<R, I: Iterator<Item = FlowFile<R>>, F1: Fn(), F2: Fn()> Drop for CloseableIter<R, I, F1, F2> {
    fn drop(&mut self) {
        if self.has_failed.load(Ordering::SeqCst) {
            (self.on_failure)()
        } else {
            (self.on_success)()
        }
    }
}

pub trait Junction: From<Vec<String>> {
    type Input;

    fn split(&self, input: &FlowFile<Self::Input>) -> u8;
}
