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

pub trait AnyTransform {
    fn transform_any(
        &self,
        input: AnyFlowFile,
    ) -> Box<dyn Iterator<Item = AnyFlowFile> + Send + '_>;
}

pub struct Pipeline<A> {
    first: A,
}

impl<A: Transform<Input = ()> + Sync> Pipeline<A> {
    pub fn new(first: A) -> Self {
        Self { first }
    }

    pub fn chain<B>(self, next: B) -> Chain<A, B>
    where
        B: Transform<Input = A::Output>,
    {
        Chain {
            first: self.first,
            next,
        }
    }
}

pub struct Chain<A, B> {
    first: A,
    next: B,
}

impl<A, B: Transform> Chain<A, B> {
    pub fn chain<C>(self, next: C) -> Chain<Chain<A, B>, C>
    where
        C: Transform<Input = B::Output>,
    {
        Chain { first: self, next }
    }
}

impl<A: Transform<Input = ()> + Sync, B: Transform<Output = ()> + Sync> Chain<A, B>
where
    B: Transform<Input = A::Output>,
{
    pub fn run(self) -> u64 {
        let genesis = ();
        let mut counter = 0;

        self.transform(genesis).for_each(|_| {
            counter += 1;
            if counter % 1_000_000 == 0 {
                eprintln!("processed {}", counter);
            }
        });

        counter
    }
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
    fn transform_any(
        &self,
        input: AnyFlowFile,
    ) -> Box<dyn Iterator<Item = AnyFlowFile> + Send + '_> {
        let iter = self
            .first
            .transform_any(input)
            .flat_map(move |o| self.next.transform_any(o));
        Box::new(iter)
    }
}
