use std::io::BufRead;

const HEADER: &str = r#"
use rayon_ingest::framework::*;
use rayon_ingest::transformers::*;
use rayon_ingest::junctions::*;

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

#[derive(PartialEq, Copy, Clone)]
enum Node {
    Transformer,
    Junction(usize),
}

fn print_pipeline(mut nodes: Vec<(usize, Node)>, total: usize) {
    while !nodes.is_empty() {
        let (i, node) = nodes.remove(0);

        if i == 0 {
            println!("    t0.start().par_bridge()");
        } else if i == total - 1 {
            println!(
                "        .for_each(|i| {{ t{}.close(i); stats.increment(); }})",
                i
            );
        } else if let Node::Junction(pos) = node {
            println!(
                r#"        .for_each(|i| match t{}.split(&i) {{
            0 => {{"#,
                i
            );
            let tail = nodes.split_off(pos + 1); // todo, hardcoded 1
            let mut nodes2: Vec<_> = nodes.split_off(pos);
            nodes.extend(tail.iter().copied());
            nodes2.extend(tail.iter().copied());

            let (ni1, _n1) = nodes.remove(0);
            // todo what if _n1 is a Junction?
            println!("    t{}.transform(i).par_bridge()", ni1);
            print_pipeline(nodes, total);
            println!("        }},");

            println!("1 => {{");
            let (ni2, _n2) = nodes2.remove(0);
            // todo what if _n2 is a Junction?
            println!("    t{}.transform(i).par_bridge()", ni2);
            print_pipeline(nodes2, total);
            println!("        }},");
            println!("_ => unreachable!(),");

            println!("        }});");

            return;
        } else {
            println!("        .flat_map(|i| t{}.transform(i).par_bridge())", i);
        }
    }
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

    let nodes: Vec<_> = lines
        .into_iter()
        .map(|(i, line)| {
            let mut words = shell_words::split(&line).unwrap();
            let mut transformer = words.remove(0);
            let node_type = if transformer.starts_with("Junction:") {
                // todo, add more junctions outputs
                let num = transformer.split_off(9).parse().unwrap();
                transformer = words.remove(0);
                Node::Junction(num)
            } else {
                Node::Transformer
            };
            let args = words
                .iter()
                .map(|s| quote(s))
                .collect::<Vec<_>>()
                .join(", ");
            println!("    let t{} = {}::from(vec![{}]);", i, transformer, args);

            (i, node_type)
        })
        .collect();

    println!();
    print_pipeline(nodes, total);

    println!("}}");
}
