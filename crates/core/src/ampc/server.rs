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

use std::sync::{Arc, Mutex};

use tokio::net::ToSocketAddrs;

use crate::distributed::sonic;
use crate::Result;

use super::{CoordReq, CoordResp, JobDht, JobReq, JobResp, Mapper, Req, Resp, Worker};
use crate::block_on;

pub struct Server<W>
where
    W: Worker,
{
    dht: Option<Arc<JobDht<W::Job>>>,
    worker: Arc<W>,
    current_job: Arc<Mutex<Option<W::Job>>>,
    conn: sonic::Server<JobReq<W::Job>, JobResp<W::Job>>,
}

impl<W> Server<W>
where
    W: Worker + 'static,
{
    async fn async_handle(&mut self) -> Result<()> {
        let mut conn = self.conn.accept().await?;
        while let Ok(req) = conn.request().await {
            match req.body().clone() {
                Req::Coordinator(coord_req) => {
                    let res = match coord_req {
                        CoordReq::CurrentJob => Resp::Coordinator(CoordResp::CurrentJob(
                            self.current_job.lock().unwrap().clone(),
                        )),
                        CoordReq::ScheduleJob { job, mapper } => {
                            *self.current_job.lock().unwrap() = Some(job.clone());
                            let worker = Arc::clone(&self.worker);
                            let dht = self.dht.clone();

                            let current_job = Arc::clone(&self.current_job);
                            std::thread::spawn(move || {
                                mapper.map(
                                    job.clone(),
                                    &worker,
                                    dht.as_ref().expect("DHT not set"),
                                );
                                current_job.lock().unwrap().take();
                            });

                            Resp::Coordinator(CoordResp::ScheduleJob(()))
                        }
                        CoordReq::Setup { dht } => {
                            self.dht = Some(Arc::new(dht));
                            Resp::Coordinator(CoordResp::Setup(()))
                        }
                    };

                    req.respond(res).await?;
                }
                Req::User(user_req) => {
                    let worker = Arc::clone(&self.worker);

                    let (tx, rx) = crossbeam_channel::bounded(1);

                    std::thread::spawn(move || {
                        let res = Resp::User(worker.handle(user_req.clone()));
                        tx.send(res).unwrap();
                    });

                    let res = tokio::task::spawn_blocking(move || rx.recv())
                        .await
                        .unwrap()?;

                    req.respond(res).await?;
                }
            };
        }

        Ok(())
    }

    fn handle(&mut self) -> Result<()> {
        block_on(self.async_handle())
    }

    pub fn bind(addr: impl ToSocketAddrs, worker: W) -> Result<Server<W>> {
        let worker = Arc::new(worker);
        let conn = block_on(sonic::Server::bind(addr))?;

        Ok(Server {
            dht: None,
            worker,
            current_job: Arc::new(Mutex::new(None)),
            conn,
        })
    }

    pub fn run(&mut self) -> Result<()> {
        loop {
            self.handle()?;
        }
    }
}
