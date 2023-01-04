// Cuely is an open source web search engine.
// Copyright (C) 2022 Cuely ApS
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

use crate::{Error, Result};

use crossbeam_channel::unbounded;
use rayon::{ThreadPool, ThreadPoolBuilder};

pub enum Executor {
    #[allow(unused)]
    SingleThread,
    ThreadPool(ThreadPool),
}

impl Executor {
    #[allow(unused)]
    pub fn single_thread() -> Executor {
        Executor::SingleThread
    }

    pub fn multi_thread(prefix: &'static str) -> Result<Executor> {
        Self::with_threads(num_cpus::get(), prefix)
    }

    pub fn with_threads(num_threads: usize, prefix: &'static str) -> Result<Executor> {
        let pool = ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .stack_size(8_000_000)
            .thread_name(move |num| format!("{prefix}{num}"))
            .build()?;
        Ok(Executor::ThreadPool(pool))
    }

    pub fn map<J: Send, R: Send, JIterator: Iterator<Item = J>, F: Sized + Sync + Fn(J) -> R>(
        &self,
        f: F,
        jobs: JIterator,
    ) -> Result<Vec<R>> {
        match self {
            Executor::SingleThread => Ok(jobs.map(f).collect::<_>()),
            Executor::ThreadPool(pool) => {
                let jobs: Vec<J> = jobs.collect();
                let num_jobs = jobs.len();
                let rx = {
                    let (tx, rx) = unbounded();
                    pool.scope(|scope| {
                        for (idx, arg) in jobs.into_iter().enumerate() {
                            let tx_ref = &tx;
                            let f_ref = &f;
                            scope.spawn(move |_| {
                                let res = f_ref(arg);
                                if let Err(err) = tx_ref.send((idx, res)) {
                                    tracing::error!(
                                        "Failed to execute job. It probably means all executor \
                                         threads have panicked. {:?}",
                                        err
                                    );
                                    panic!();
                                }
                            });
                        }
                    });
                    rx
                    // This ends the scope of tx.
                    // This is important as it makes it possible for the rx iteration to
                    // terminate.
                };
                let mut result_placeholders: Vec<Option<R>> =
                    std::iter::repeat_with(|| None).take(num_jobs).collect();

                for (pos, res) in rx {
                    result_placeholders[pos] = Some(res);
                }
                let results: Vec<R> = result_placeholders.into_iter().flatten().collect();

                if results.len() != num_jobs {
                    return Err(Error::InternalError(
                        "At least one of the scheduled jobs failed.".to_string(),
                    ));
                }

                Ok(results)
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::Executor;

    #[test]
    #[should_panic(expected = "panic should propagate")]
    fn test_panic_propagates_single_thread() {
        let _result: Vec<usize> = Executor::single_thread()
            .map(
                |_| {
                    panic!("panic should propagate");
                },
                vec![0].into_iter(),
            )
            .unwrap();
    }

    #[test]
    #[should_panic] //< unfortunately the panic message is not propagated
    fn test_panic_propagates_multi_thread() {
        let _result: Vec<usize> = Executor::with_threads(1, "search-test")
            .unwrap()
            .map(
                |_| {
                    panic!("panic should propagate");
                },
                vec![0].into_iter(),
            )
            .unwrap();
    }

    #[test]
    fn test_map_singlethread() {
        let result: Vec<usize> = Executor::single_thread().map(|i| i * 2, 0..1_000).unwrap();
        assert_eq!(result.len(), 1_000);

        for (i, r) in result.iter().enumerate() {
            assert_eq!(*r, i * 2);
        }
    }

    #[test]
    fn test_map_multithread() {
        let result: Vec<usize> = Executor::with_threads(3, "search-test")
            .unwrap()
            .map(|i| i * 2, 0..10)
            .unwrap();
        assert_eq!(result.len(), 10);
        for (i, r) in result.iter().enumerate() {
            assert_eq!(*r, i * 2);
        }
    }
}
