use std::any::Any;

pub struct AnyFlowFile {
    pub data: Box<dyn Any + Sync + Send + 'static>,
    pub source: String,
}

pub trait AnyTransform {
    fn transform(&self, input: AnyFlowFile) -> Box<dyn Iterator<Item = AnyFlowFile> + Send + '_>;
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
