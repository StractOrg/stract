use std::{cmp::Ordering, collections::HashSet};

use anyhow::Result;
use heck::ToSnakeCase;
use itertools::Itertools;
use xshell::{cmd, Shell};

#[derive(Debug)]
struct DepTree {
    name: String,
    depth: u32,
    deps: Vec<DepTree>,
}

fn main() -> Result<()> {
    let sh = Shell::new()?;

    let tree = cmd!(sh, "cargo tree --prefix depth").read()?;

    let lines = tree
        .lines()
        .filter_map(|l| {
            let s = l.split_once("/stract/")?.0.split_once(' ')?.0;
            Some((s[0..1].parse::<usize>().unwrap(), s[1..].to_string()))
        })
        .collect_vec();

    let mut path: Vec<String> = Vec::new();

    let mut nodes = HashSet::new();
    let mut edges = HashSet::new();

    for (depth, name) in lines {
        path.truncate(depth);
        nodes.insert(name.clone());
        if let Some(parent) = path.last() {
            edges.insert((parent.clone(), name.clone()));
        }
        path.push(name);
    }

    println!("digraph G {{");
    for n in nodes {
        if n == "xtask" {
            continue;
        }
        println!("  {}[label={n:?}]", n.to_snake_case());
    }
    for (a, b) in edges.iter().sorted() {
        println!("  {} -> {}", a.to_snake_case(), b.to_snake_case());
    }
    println!("}}");

    // println!("flowchart TD");
    // for (a, b) in edges.iter().sorted() {
    //     println!("  {} --> {}", a.to_snake_case(), b.to_snake_case());
    // }

    Ok(())
}
