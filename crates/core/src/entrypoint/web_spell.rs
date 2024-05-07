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

use std::path::{Path, PathBuf};

use anyhow::Result;
use fnv::FnvHashMap;
use itertools::Itertools;

use tracing::{debug, info};
use whatlang::Lang;

use crate::{
    config::{self, WebSpellConfig},
    entrypoint::download_all_warc_files,
    web_spell::{FirstTrainer, FirstTrainerResult, SecondTrainer},
    webpage::Html,
};

pub struct SpellWorker {
    prev_trained: Vec<FnvHashMap<Lang, FirstTrainerResult>>,
    languages: Vec<Lang>,
    path: PathBuf,
}

impl SpellWorker {
    pub fn new<P: AsRef<Path>>(languages: &[Lang], path: P) -> Result<Self> {
        Ok(Self {
            path: path.as_ref().to_path_buf(),
            languages: languages.to_vec(),
            prev_trained: Vec::new(),
        })
    }

    pub fn process(&mut self, job: Job) -> Result<()> {
        let name = job.warc_path.split('/').last().unwrap();

        info!("processing {}", name);

        let mut trainer_has_inserts = false;
        let mut trainer: FnvHashMap<Lang, FirstTrainer> = Default::default();

        for lang in &self.languages {
            trainer.insert(
                *lang,
                FirstTrainer::new(self.path.as_path().join(name).join(lang.to_string()))?,
            );
        }

        let source = job.source_config.clone();

        let paths = vec![job.warc_path.clone()];
        let warc_files = download_all_warc_files(&paths, &source);
        tokio::pin!(warc_files);

        for file in warc_files.by_ref() {
            for record in file.records().flatten() {
                let webpage = match Html::parse(&record.response.body, &record.request.url) {
                    Ok(webpage) => webpage,
                    Err(err) => {
                        tracing::error!("error parsing webpage: {}", err);
                        continue;
                    }
                };

                let lang = match webpage.lang() {
                    Some(lang) => *lang,
                    None => continue,
                };

                match (webpage.clean_text(), trainer.get_mut(&lang)) {
                    (Some(text), Some(trainer)) => {
                        trainer.add(text);
                        trainer_has_inserts = true;
                    }
                    _ => continue,
                }
            }
        }
        if trainer_has_inserts {
            self.finish_training_step(trainer)?;
        }

        info!("finished processing {}", name);

        Ok(())
    }

    fn finish_training_step(&mut self, trainer: FnvHashMap<Lang, FirstTrainer>) -> Result<()> {
        debug!("next training step");
        let mut trained = FnvHashMap::default();

        for (lang, trainer) in trainer {
            trained.insert(lang, trainer.next_training_step()?);
        }

        self.prev_trained.push(trained);
        debug!("next training step created");
        Ok(())
    }

    fn next_training_steps(self) -> Vec<FnvHashMap<Lang, FirstTrainerResult>> {
        self.prev_trained
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, bincode::Encode, bincode::Decode)]
pub struct Job {
    pub source_config: config::WarcSource,
    pub warc_path: String,
}

pub fn run(config: WebSpellConfig) -> Result<()> {
    let warc_paths = config.warc_source.paths()?;

    let jobs: Vec<_> = warc_paths
        .into_iter()
        .take(config.limit_warc_files.unwrap_or(usize::MAX))
        .map(|warc_path| Job {
            source_config: config.warc_source.clone(),
            warc_path,
        })
        .collect_vec();

    let num_workers = usize::from(std::thread::available_parallelism()?).min(jobs.len());
    let mut handlers = Vec::new();

    for i in 0..num_workers {
        let path = Path::new(&config.output_path).join(format!("{i}"));
        std::fs::create_dir_all(&path)?;
        let mut worker = SpellWorker::new(&config.languages, path)?;

        let jobs = jobs.clone();
        handlers.push(std::thread::spawn(move || {
            for job in jobs.into_iter().skip(i).step_by(num_workers) {
                worker.process(job).unwrap();
            }

            worker.next_training_steps()
        }));
    }

    let mut combined: FnvHashMap<Lang, Vec<FirstTrainerResult>> = FnvHashMap::default();
    for handler in handlers {
        let trained = handler.join().unwrap();

        for res in trained {
            for (lang, result) in res {
                combined.entry(lang).or_default().push(result);
            }
        }
    }

    for (lang, results) in combined {
        info!("creating second trainer for {}", lang);
        let second_trainer = SecondTrainer::new(
            results,
            Path::new(&config.output_path)
                .join("checker")
                .join(lang.code()),
        )?;
        debug!("second trainer created");

        second_trainer.train()?;
    }

    Ok(())
}
