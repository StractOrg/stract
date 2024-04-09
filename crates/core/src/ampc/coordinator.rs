// Stract is an open source web search engine.
// Copyright (C) 2024 Stract ApS
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
// along with this program.  If not, see <https://www.gnu.org/licenses/>

use rayon::prelude::*;
use std::{
    collections::{BTreeMap, VecDeque},
    time::Duration,
};

use super::{
    DhtConn, ExponentialBackoff, Finisher, Job, JobScheduled, RemoteWorker, Setup, Worker,
    WorkerRef,
};
use crate::Result;
use anyhow::anyhow;

pub struct Coordinator<J>
where
    J: Job,
{
    workers: BTreeMap<WorkerRef, <<J as Job>::Worker as Worker>::Remote>,
    setup: Box<dyn Setup<DhtTables = J::DhtTables>>,
    mappers: Vec<J::Mapper>,
}

impl<J> Coordinator<J>
where
    J: Job,
{
    pub fn new<S>(setup: S, workers: Vec<<<J as Job>::Worker as Worker>::Remote>) -> Self
    where
        S: Setup<DhtTables = J::DhtTables> + 'static,
    {
        Self {
            setup: Box::new(setup),
            mappers: Vec::new(),
            workers: workers
                .into_iter()
                .enumerate()
                .map(|(i, w)| (WorkerRef(i), w))
                .collect(),
        }
    }

    pub fn with_mapper(mut self, mapper: J::Mapper) -> Self {
        self.mappers.push(mapper);
        self
    }

    fn send_dht_to_workers(&self, dht: &DhtConn<J::DhtTables>) -> Result<()> {
        self.workers
            .par_iter()
            .map(|(_, worker)| worker.send_dht(dht))
            .collect::<Result<()>>()?;

        Ok(())
    }

    fn schedule_job(
        &self,
        job: J,
        mapper: J::Mapper,
        worker_jobs: &BTreeMap<WorkerRef, Result<Option<J>>>,
    ) -> Result<JobScheduled> {
        let potential_workers = worker_jobs
            .iter()
            .filter_map(|(r, j)| j.as_ref().ok().map(|_| *r))
            .filter(|r| job.is_schedulable(&self.workers[r]))
            .collect::<Vec<_>>();

        if potential_workers.is_empty() {
            return Err(anyhow!(
                "Failed to schedule job: no potential workers are responding."
            ));
        }

        // schedule remaining jobs to idle workers (if any)
        match potential_workers.iter().find(|r| {
            worker_jobs[r]
                .as_ref()
                .expect("references in `potential_workers` should only point to non-failed workers")
                .is_none()
        }) {
            Some(free_worker) => {
                self.workers[free_worker].schedule_job(&job, mapper)?;
                Ok(JobScheduled::Success(*free_worker))
            }
            None => Ok(JobScheduled::NoAvailableWorkers),
        }
    }

    fn await_scheduled_jobs(
        &self,
        mut scheduled_jobs: BTreeMap<WorkerRef, J>,
        mapper: J::Mapper,
    ) -> Result<()> {
        tracing::debug!("Awaiting scheduled jobs");
        let mut sleeper =
            ExponentialBackoff::new(Duration::from_millis(100), Duration::from_secs(10), 2.0);

        loop {
            let worker_jobs = self
                .workers
                .iter()
                .map(|(r, w)| (*r, w.current_job()))
                .collect::<BTreeMap<_, _>>();

            let mut finished_workers = 0;
            for (r, j) in worker_jobs.iter() {
                match j {
                    Ok(Some(_)) => break,
                    Ok(None) => finished_workers += 1,
                    Err(_) => {
                        if let Some(job) = &scheduled_jobs.remove(r) {
                            match self.schedule_job(job.clone(), mapper.clone(), &worker_jobs)? {
                                JobScheduled::Success(r) => {
                                    scheduled_jobs.insert(r, job.clone());
                                    break; // need to break to avoid double scheduling to same worker
                                }
                                JobScheduled::NoAvailableWorkers => {
                                    // retry scheduling.
                                    break;
                                }
                            }
                        }
                    }
                }
            }

            if finished_workers == self.workers.len() {
                break;
            }

            std::thread::sleep(sleeper.next());
        }

        Ok(())
    }

    pub fn run<F>(self, jobs: Vec<J>, finisher: F) -> Result<J::DhtTables>
    where
        F: Finisher<Job = J>,
    {
        let mut dht = self.setup.init_dht();
        dht.cleanup_prev_tables();

        self.setup.setup_first_round(dht.prev());
        self.setup.setup_first_round(dht.next());

        while !finisher.is_finished(dht.prev()) {
            tracing::debug!("Starting new round");
            self.setup.setup_round(dht.next());
            self.send_dht_to_workers(&dht)?;

            for mapper in &self.mappers {
                // run round
                let mut remaining_jobs: VecDeque<_> = jobs.clone().into_iter().collect();
                let mut sleeper = ExponentialBackoff::new(
                    Duration::from_millis(100),
                    Duration::from_secs(10),
                    2.0,
                );

                let mut scheduled_jobs: BTreeMap<WorkerRef, J> = BTreeMap::new();

                while let Some(job) = remaining_jobs.pop_front() {
                    // get current status from workers
                    let worker_jobs = self
                        .workers
                        .iter()
                        .map(|(r, w)| (*r, w.current_job()))
                        .collect::<BTreeMap<_, _>>();

                    // handle failed workers
                    for r in worker_jobs
                        .iter()
                        .filter_map(|(r, j)| j.as_ref().err().map(|_| r))
                    {
                        if let Some(job) = scheduled_jobs.remove(r) {
                            remaining_jobs.push_front(job);
                        }
                    }

                    match self.schedule_job(job.clone(), mapper.clone(), &worker_jobs)? {
                        JobScheduled::Success(worker) => {
                            scheduled_jobs.insert(worker, job);
                            sleeper.success();
                        }
                        JobScheduled::NoAvailableWorkers => {
                            remaining_jobs.push_front(job);
                            let sleep_duration = sleeper.next();
                            std::thread::sleep(sleep_duration);
                        }
                    }
                }

                self.await_scheduled_jobs(scheduled_jobs, mapper.clone())?;
            }

            dht.next_round();
        }

        Ok(dht.take_prev())
    }
}
