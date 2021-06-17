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
    fn it_works() {
        let g = Glob {
            patterns: vec!["*.toml".into()],
        };
        let u = Unpack {};
        let l = Lines {};
        let n = Nullify::default();

        let a1 = Identity::default();
        let a2 = Identity::default();
        let b2 = Identity::default();

        let counter = AtomicU64::new(0);

        g.transform(())
            .par_bridge()
            .flat_map(|i| u.transform(i).par_bridge())
            .for_each(|i| {
                if true {
                    a1.transform(i)
                        .par_bridge()
                        //-- same
                        .flat_map(|i| l.transform(i).par_bridge())
                        .flat_map(|i| n.transform(i).par_bridge())
                        .for_each(|()| {
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
                        .flat_map(|i| n.transform(i).par_bridge())
                        .for_each(|()| {
                            let cur = counter.fetch_add(1, Ordering::Relaxed);
                            if cur % 1_000_000 == 0 {
                                eprintln!("processed {}", cur);
                            }
                        })
                }
            });

        let count = counter.load(Ordering::Relaxed);
        assert_eq!(count, 12);
    }
}
