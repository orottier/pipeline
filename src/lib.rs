#![feature(min_type_alias_impl_trait)]

pub mod framework;
pub mod transformers;

#[cfg(test)]
mod tests {
    use crate::framework::*;
    use crate::transformers::*;

    use rayon::iter::ParallelBridge;
    use rayon::prelude::ParallelIterator;

    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn test_nonlinear_flow() {
        let g = Glob::from(vec!["*.toml".to_string()]);
        let u = Unpack {};
        let l = Lines {};
        let n = Nullify::from(vec![]);

        let a1 = Identity::default();
        let a2 = Identity::default();
        let b2 = Identity::default();

        let counter = AtomicU64::new(0);

        g.start()
            .par_bridge()
            .flat_map(|i| u.transform(i).par_bridge())
            .for_each(|i| {
                if true {
                    a1.transform(i)
                        .par_bridge()
                        //-- same
                        .flat_map(|i| l.transform(i).par_bridge())
                        .for_each(|i| {
                            n.close(i);
                            let cur = counter.fetch_add(1, Ordering::Relaxed);
                            if cur % 1_000_000 == 0 {
                                eprintln!("processed {}", cur);
                            }
                        })
                } else {
                    a2.transform(i)
                        .par_bridge()
                        .flat_map(|i| b2.transform(i).par_bridge())
                        //-- same
                        .flat_map(|i| l.transform(i).par_bridge())
                        .for_each(|i| {
                            n.close(i);
                            let cur = counter.fetch_add(1, Ordering::Relaxed);
                            if cur % 1_000_000 == 0 {
                                eprintln!("processed {}", cur);
                            }
                        })
                }
            });

        let count = counter.load(Ordering::Relaxed);
        assert_eq!(count, 15);
    }
}
