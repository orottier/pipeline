use crate::framework::*;

use std::marker::PhantomData;

pub struct SplitByExt<A> {
    exts: Vec<String>,
    _marker: PhantomData<A>,
}

impl<A> From<Vec<String>> for SplitByExt<A> {
    fn from(args: Vec<String>) -> Self {
        Self {
            exts: args,
            _marker: PhantomData,
        }
    }
}

impl<A> Junction for SplitByExt<A> {
    type Input = A;

    fn split(&self, input: &FlowFile<Self::Input>) -> u8 {
        let ext = input.meta.source().split('.').last().unwrap();

        let pos = match self.exts.iter().position(|v| v == ext) {
            None => return 255,
            Some(pos) => pos,
        };

        pos as u8
    }
}
