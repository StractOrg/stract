// Stract is an open source web search engine.
// Copyright (C) 2023 Stract ApS
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use std::fmt::Display;
use std::{
    collections::{HashSet, VecDeque},
    path::Path,
    str::FromStr,
};

use csv::Writer;
use indicatif::{ProgressBar, ProgressStyle};

use crate::Result;

fn suggestions(query: &str, gl: &str) -> Result<Vec<String>> {
    let url = format!(
        "https://www.google.com/complete/search?q={}&gl={}&client=gws-wiz&xssi=t",
        urlencoding::encode(query),
        gl
    );

    let client = reqwest::blocking::Client::new();
    let builder = client.get(url);

    let input = builder.send()?.text()?;

    let mut input = input.split('\n');
    input.next();
    let input = input.next().expect("None option");

    let output: serde_json::Value = serde_json::from_str(input)?;
    let mut suggestions = Vec::new();

    if let serde_json::Value::Array(arr) = output {
        if let serde_json::Value::Array(arr) = arr[0].clone() {
            for result in arr {
                if let serde_json::Value::Array(result) = result {
                    if let serde_json::Value::String(result) = result[0].clone() {
                        let result = result.replace("<b>", "").replace("</b>", "");
                        suggestions.push(result);
                    }
                }
            }
        }
    }

    Ok(suggestions)
}

#[derive(Clone)]
pub enum Gl {
    Us,
}

impl Display for Gl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let code = match self {
            Gl::Us => "us",
        };
        write!(f, "{code}")
    }
}

impl FromStr for Gl {
    type Err = crate::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "us" => Ok(Gl::Us),
            _ => Err(crate::Error::UnknownCLIOption),
        }
    }
}

fn save_queries<P: AsRef<Path>>(queries: &HashSet<String>, path: P) -> Result<()> {
    let mut wtr = Writer::from_path(&path)?;

    let mut queries: Vec<_> = queries.iter().collect();
    queries.sort();

    for query in queries {
        wtr.write_record([query])?;
    }

    wtr.flush()?;

    Ok(())
}

pub fn run<P: AsRef<Path>>(
    queries_to_scrape: usize,
    gl: Gl,
    ms_sleep_between_req: u64,
    output_dir: P,
) -> Result<()> {
    let mut queries = HashSet::new();
    let mut queue = VecDeque::new();

    let pb = ProgressBar::new(queries_to_scrape as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{wide_bar}] {pos:>7}/{len:7} ({eta})")
            .unwrap()
            .progress_chars("#>-"),
    );

    for c in 'a'..='z' {
        queue.push_back(c.to_string());
    }

    let path = output_dir
        .as_ref()
        .join(format!("queries_{:}.csv", gl.to_string().as_str()));

    let mut queries_since_last_save = 0;

    while let Some(query) = queue.pop_front() {
        let res = suggestions(&query, gl.to_string().as_str());

        if res.is_err() {
            continue;
        }

        let res = res.unwrap();
        let mut new_queries = 0;

        for next_query in res {
            if queries.contains(&next_query) {
                continue;
            }

            let mut new = Vec::new();
            for c in next_query.chars() {
                let q = new.clone().into_iter().collect();
                new.push(c);

                if !queries.contains(&q) {
                    queue.push_back(q);
                }
            }

            for q in next_query.split_whitespace() {
                if !queries.contains(q) {
                    queries.insert(q.to_string());
                    queue.push_back(q.to_string());
                }
            }

            new_queries += 1;
            queries.insert(next_query);
            pb.tick();
            pb.set_position(queries.len() as u64);
        }

        if queries.len() >= queries_to_scrape {
            break;
        }

        queries_since_last_save += new_queries;

        if queries_since_last_save > 1_000 {
            save_queries(&queries, &path)?;
            queries_since_last_save = 0;
        }

        std::thread::sleep(std::time::Duration::from_millis(ms_sleep_between_req));
    }

    pb.finish();

    save_queries(&queries, &path)?;

    Ok(())
}
