pub struct FlowFile<T> {
    pub data: T,
    pub source: String,
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
