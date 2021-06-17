pub struct FlowFile<T> {
    pub data: T,
    pub source: String,
}

pub trait Transform {
    type Input;
    type Output;

    fn transform(&self, input: Self::Input) -> Box<dyn Iterator<Item = Self::Output> + Send + '_>;
}
