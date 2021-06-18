use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

pub struct FlowFile<T> {
    pub data: T,
    pub source: String,
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

    fn report(&self) {
        let total = self.total.load(Ordering::Relaxed);
        let elapsed = SystemTime::now().duration_since(self.start).unwrap();
        let rate = total * 1000 / elapsed.as_millis() as u64;
        log::info!("Processed {:10} items at {:10} msgs/sec", total, rate);
    }
}

impl Drop for Stats {
    fn drop(&mut self) {
        self.report()
    }
}

pub trait Transform {
    type Input;
    type Output;
    type Iter: Iterator<Item = FlowFile<Self::Output>> + Send;

    fn transform(&self, input: FlowFile<Self::Input>) -> Self::Iter;
}

pub trait StartTransform {
    type Output;
    type Iter: Iterator<Item = FlowFile<Self::Output>> + Send;

    fn start(&self) -> Self::Iter;
}

pub trait CloseTransform {
    type Input;

    fn close(&self, input: FlowFile<Self::Input>);
}

pub struct CloseableIter<I: Iterator, F: Fn()> {
    iter: I,
    after: F,
}

impl<I: Iterator, F: Fn()> CloseableIter<I, F> {
    pub fn new(iter: I, after: F) -> Self {
        Self { iter, after }
    }
}

impl<I: Iterator, F: Fn()> Iterator for CloseableIter<I, F> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl<I: Iterator, F: Fn()> Drop for CloseableIter<I, F> {
    fn drop(&mut self) {
        (self.after)()
    }
}
