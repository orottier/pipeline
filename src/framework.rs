pub struct FlowFile<T> {
    pub data: T,
    pub source: String,
}

impl FlowFile<()> {
    pub fn genesis() -> Self {
        Self {
            data: (),
            source: String::new(),
        }
    }
}

pub trait Transform {
    type Input;
    type Output;
    type Iter: Iterator<Item = FlowFile<Self::Output>> + Send;

    fn transform(&self, input: FlowFile<Self::Input>) -> Self::Iter;
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
