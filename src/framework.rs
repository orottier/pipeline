use std::any::Any;

pub struct FlowFile {
    pub data: Box<dyn Any + Sync + Send + 'static>,
    pub source: String,
}

pub trait Transform {
    fn transform(&self, input: FlowFile) -> Box<dyn Iterator<Item = FlowFile> + Send + '_>;
}

pub struct ChainAny {
    pub first: Box<dyn Transform + Send + Sync + 'static>,
    pub next: Box<dyn Transform + Send + Sync + 'static>,
}

impl Transform for ChainAny {
    fn transform(&self, input: FlowFile) -> Box<dyn Iterator<Item = FlowFile> + Send + '_> {
        let iter = self
            .first
            .transform(input)
            .flat_map(move |o| self.next.transform(o));
        Box::new(iter)
    }
}
