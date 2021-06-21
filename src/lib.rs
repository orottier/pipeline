#![feature(min_type_alias_impl_trait)]

pub mod framework;
pub mod junctions;
pub mod transformers;

#[cfg(test)]
mod tests {
    use crate::framework::*;
    use crate::junctions::*;
    use crate::transformers::*;

    use rayon::iter::ParallelBridge;
    use rayon::prelude::ParallelIterator;

    #[test]
    fn test_nonlinear_flow() {
        let g = Glob::from(vec!["*.toml".to_string(), "a.csv".to_string()]);
        let u = Unpack {};
        let s = SplitByExt::from(vec!["toml".to_string(), "csv".to_string()]);
        let l = Lines {};
        let c = Csv {};
        let t = ToString::default();
        let n = Nullify::from(vec![]);

        let stats = Stats::new();

        g.start()
            .par_bridge()
            .flat_map(|i| u.transform(i).par_bridge())
            .for_each(|i| match s.split(&i) {
                0 => l.transform(i).par_bridge().for_each(|i| {
                    n.close(i);
                    stats.increment();
                }),
                1 => c
                    .transform(i)
                    .par_bridge()
                    .flat_map(|i| t.transform(i).par_bridge())
                    .for_each(|i| {
                        n.close(i);
                        stats.increment();
                    }),
                _ => unreachable!(),
            });

        let count = stats.total();
        assert_eq!(count, 18);
    }
}
