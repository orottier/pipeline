use std::io::BufRead;

const HEADER: &str = r#"
use rayon_ingest::framework::*;
use rayon_ingest::transformers::*;

use rayon::iter::ParallelBridge;
use rayon::prelude::ParallelIterator;

use env_logger::Env;

fn main() {
    // setup logger, DEBUG level by default
    env_logger::Builder::from_env(Env::default().default_filter_or("debug")).init();

    let stats = Stats::new();
"#;

fn quote(s: &str) -> String {
    let mut quoted = "String::from(\"".to_string();
    for c in s.chars() {
        if c == '"' {
            quoted.push_str("\\\"");
        } else {
            quoted.push(c);
        }
    }
    quoted.push_str("\")");

    quoted
}

pub fn main() {
    let stdin = std::io::stdin();
    let lines: Vec<_> = stdin
        .lock()
        .lines()
        .flat_map(Result::ok)
        .enumerate()
        .collect();
    let total = lines.len();

    println!("{}", HEADER);

    lines.iter().for_each(|(i, line)| {
        let mut words = shell_words::split(&line).unwrap();
        let transformer = words.remove(0);
        let args = words
            .iter()
            .map(|s| quote(s))
            .collect::<Vec<_>>()
            .join(", ");
        println!("    let t{} = {}::from(vec![{}]);", i, transformer, args);
    });

    lines.iter().for_each(|(i, _line)| {
        if *i == 0 {
            println!("    t0.start().par_bridge()");
        } else if *i == total - 1 {
            println!(
                "        .for_each(|i| {{ t{}.close(i); stats.increment(); }})",
                i
            );
        } else {
            println!("        .flat_map(|i| t{}.transform(i).par_bridge())", i);
        }
    });

    println!("}}");
}
