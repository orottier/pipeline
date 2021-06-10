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
            .transform_any(genesis)
            .par_bridge()
            .flat_map(|i| u.transform_any(i).par_bridge())
            .flat_map(|i| l.transform_any(i).par_bridge())
            .count();

        assert_eq!(a, 12);
    }

    #[test]
    fn chain() {
        let g = Glob {
            patterns: vec!["*.toml".into()],
        };
        let u = Unpack {};
        let l = Lines {};
        let n = Nullify::default();

        let a = Chain { first: g, next: u };
        let a = a.chain(l).chain(n);

        assert_eq!(a.run(), 12);
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

        let c = a.transform_any(genesis).count();
        assert_eq!(c, 12);
    }
}
