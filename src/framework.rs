use std::any::Any;

pub struct FlowFile<T> {
    pub data: T,
    pub source: String,
}

pub struct AnyFlowFile {
    pub data: Box<dyn Any + Sync + Send + 'static>,
    pub source: String,
}

pub trait Transform {
    type Input;
    type Output;

    fn transform(&self, input: Self::Input) -> Box<dyn Iterator<Item = Self::Output> + Send + '_>;
}

impl<O> dyn Transform<Input=(), Output=O> {
    //fn connect
}

pub trait AnyTransform {
    fn transform(&self, input: AnyFlowFile) -> Box<dyn Iterator<Item = AnyFlowFile> + Send + '_>;
}

pub struct Chain<A, B> {
    pub first: A,
    pub next: B,
}

impl<A: Transform + Sync, B: Sync> Transform for Chain<A, B>
where
    B: Transform<Input = A::Output>,
{
    type Input = A::Input;
    type Output = B::Output;

    fn transform(&self, input: Self::Input) -> Box<dyn Iterator<Item = Self::Output> + Send + '_> {
        let iter = self
            .first
            .transform(input)
            .flat_map(move |o| self.next.transform(o));
        Box::new(iter)
    }
}

pub struct AnyChain {
    pub first: Box<dyn AnyTransform + Send + Sync + 'static>,
    pub next: Box<dyn AnyTransform + Send + Sync + 'static>,
}

impl AnyTransform for AnyChain {
    fn transform(&self, input: AnyFlowFile) -> Box<dyn Iterator<Item = AnyFlowFile> + Send + '_> {
        let iter = self
            .first
            .transform(input)
            .flat_map(move |o| self.next.transform(o));
        Box::new(iter)
    }
}
