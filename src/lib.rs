pub mod framework;
pub mod transformers;

#[cfg(test)]
mod tests {
    use crate::framework::*;
    use crate::transformers::*;

    use rayon::iter::ParallelBridge;
    use rayon::prelude::ParallelIterator;

    #[test]
    fn it_works() {
        let g = Glob {
            patterns: vec!["*.toml".into()],
        };
        let u = Unpack {};
        let l = Lines {};

        let genesis = AnyFlowFile {
            data: Box::new(()),
            source: "".into(),
        };
        let a = g
            .transform(genesis)
            .par_bridge()
            .flat_map(|i| u.transform(i).par_bridge())
            .flat_map(|i| l.transform(i).par_bridge())
            .count();

        assert_eq!(a, 12);
    }

    #[test]
    fn chain_any() {
        let g = Glob {
            patterns: vec!["*.toml".into()],
        };
        let u = Unpack {};
        let l = Lines {};

        let genesis = AnyFlowFile {
            data: Box::new(()),
            source: "".into(),
        };
        let mut a: AnyChain = AnyChain {
            first: Box::new(g),
            next: Box::new(u),
        };
        a = AnyChain {
            first: Box::new(a),
            next: Box::new(l),
        };

        let c = a.transform(genesis).count();
        assert_eq!(c, 12);
    }
}
