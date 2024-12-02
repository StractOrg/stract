use indicatif::ParallelProgressIterator;
use rayon::prelude::*;

use crate::error_model::ErrorModel;

use super::{stupid_backoff::StupidBackoffTrainer, tokenize, Result};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use super::{stupid_backoff::StupidBackoff, term_freqs::TermDict};

pub struct FirstTrainer {
    term_dict: TermDict,
    lm_model: StupidBackoffTrainer,

    path: PathBuf,
}

impl FirstTrainer {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        Ok(Self {
            term_dict: TermDict::open(path.as_ref().join("term_dict"))?,
            lm_model: StupidBackoffTrainer::new(3),
            path: path.as_ref().to_path_buf(),
        })
    }

    pub fn add(&mut self, text: &str) {
        let tokens = tokenize(text);
        for term in &tokens {
            self.term_dict.insert(term);
        }

        self.lm_model.train(&tokens);
    }

    pub fn next_training_step(mut self) -> Result<FirstTrainerResult> {
        self.term_dict.commit()?;
        self.term_dict.merge_dicts()?;
        self.lm_model.build(self.path.join("stupid_backoff"))?;

        Ok(FirstTrainerResult {
            term_dict: self.term_dict,
            lm_model: StupidBackoff::open(self.path.join("stupid_backoff"))?,
        })
    }
}

pub struct FirstTrainerResult {
    term_dict: TermDict,
    lm_model: StupidBackoff,
}

/// Constructs the error model from the first training step.
pub struct SecondTrainer {
    term_dict: TermDict,
    lm_model: StupidBackoff,

    path: PathBuf,
}

impl SecondTrainer {
    /// Panics if vec is empty.
    pub fn new<P: AsRef<Path>>(first_steps: Vec<FirstTrainerResult>, path: P) -> Result<Self> {
        if !path.as_ref().exists() {
            std::fs::create_dir_all(path.as_ref())?;
        }

        let (lm_models, mut term_dicts): (Vec<_>, Vec<_>) = first_steps
            .into_iter()
            .map(|r| (r.lm_model, r.term_dict))
            .unzip();

        let lm_model = StupidBackoff::merge(lm_models, path.as_ref().join("stupid_backoff"))?;

        let mut term_dict = term_dicts.pop().unwrap();

        for other in term_dicts {
            term_dict.merge(other)?;
        }
        tracing::debug!("merged term dicts");

        std::fs::rename(term_dict.path(), path.as_ref().join("term_dict"))?;
        drop(term_dict);

        let mut term_dict = TermDict::open(path.as_ref().join("term_dict"))?;
        term_dict.merge_dicts()?;

        Ok(Self {
            term_dict,
            lm_model,
            path: path.as_ref().to_path_buf(),
        })
    }

    pub fn train(self) -> Result<()> {
        tracing::info!("training error model");
        let terms = self.term_dict.terms();

        let errors: Vec<_> = terms
            .into_par_iter()
            .progress()
            .map(|term| {
                // one edit for words of
                // up to four characters, two edits for up to twelve
                // characters, and three for longer
                let max_edit_distance = if term.len() <= 4 {
                    1
                } else if term.len() <= 12 {
                    2
                } else {
                    3
                };

                let possible_corrections = self
                    .term_dict
                    .search(&term, max_edit_distance)
                    .into_iter()
                    .filter(|correction| {
                        correction != &term
                            && 10 * self.term_dict.freq(&term).unwrap_or_default()
                                < self.term_dict.freq(correction).unwrap_or_default()
                    })
                    .collect::<Vec<_>>();

                (term, possible_corrections)
            })
            .filter(|(_, errors)| !errors.is_empty())
            .map(|(term, possible_corrections)| {
                let contexts = self.lm_model.contexts(&term);

                let best_terms = contexts
                    .into_iter()
                    .filter_map(
                        |(context, count)| {
                            if count < 10 {
                                None
                            } else {
                                Some(context)
                            }
                        },
                    )
                    .filter(|c| c.len() == 3)
                    .map(|context| {
                        let most_probable = possible_corrections
                            .iter()
                            .chain(vec![&term])
                            .map(|t| {
                                let mut words = context.clone();
                                words[1].clone_from(t);
                                (self.lm_model.freq(&words), t)
                            })
                            .max_by(|(a, _), (b, _)| a.cmp(b))
                            .unwrap()
                            .1
                            .clone();

                        most_probable
                    })
                    .collect::<Vec<_>>();

                // count number of contexts for each best_term
                let mut counts: HashMap<String, u64> = HashMap::new();

                for t in best_terms {
                    *counts.entry(t).or_insert(0) += 1;
                }

                (term, counts)
            })
            .collect();

        let mut error_model = ErrorModel::new();

        for (term, possible_correction) in errors
            .into_iter()
            .flat_map(|(term, possible_corrections)| {
                possible_corrections
                    .into_keys()
                    .map(move |other| (term.clone(), other.clone()))
            })
            .filter(|(a, b)| a != b)
        {
            error_model.add(&term, &possible_correction);
        }

        error_model.save(self.path.join("error_model.json"))?;

        Ok(())
    }
}
