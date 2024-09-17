use std::sync::{Arc, RwLock};

use super::{
    tcp::{tcp_worker_init, TCPWriter},
    RequestState,
};
use crate::{implement::dpdk::tcp::tcp_worker_run, interface::SocketHandle};
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct Repeater {
    pub _tcp_sender: Option<Arc<std::thread::JoinHandle<()>>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ChunkInfo {
    pub chunk_offset: i32,
    pub chunk_size: i32,
}

impl Repeater {
    pub fn new(
        rank: i32,
        nranks: i32,
        msg_receiver: flume::Receiver<(i32, &'static [u8], Arc<RwLock<RequestState>>)>,
        socket: SocketHandle,
    ) -> Self {
        if cfg!(not(feature = "storage")) || (rank != 0 && rank != nranks - 1) {
            return Self { _tcp_sender: None };
        }

        let _tcp_sender = Some(Arc::new(std::thread::spawn(move || {
            let mut worker = tcp_worker_init();
            let mut ctrl_writer = TCPWriter::new(&mut worker, socket.clone(), None);
            let mut data_writer = TCPWriter::new(&mut worker, socket.clone(), None);

            loop {
                tcp_worker_run(&mut worker);
                if let Ok((chunk_offset, buf, state)) = msg_receiver.try_recv() {
                    let chunk_info = ChunkInfo {
                        chunk_offset,
                        chunk_size: buf.len() as i32,
                    };
                    let info_bytes = bincode::serialize(&chunk_info).unwrap();
                    ctrl_writer
                        .tcp_write(&mut worker, &info_bytes)
                        .expect("failed to write chunk info");

                    data_writer
                        .tcp_write(&mut worker, buf)
                        .expect("failed to write data");

                    match state.write() {
                        Ok(mut state) => {
                            state.completed_subtasks += 1;
                        }
                        Err(poisoned) => {
                            println!("Poisoned lock");
                            tracing::warn!("{:?}", poisoned);
                        }
                    };
                }
            }
        })));
        Self { _tcp_sender }
    }
}

impl Drop for Repeater {
    fn drop(&mut self) {
        if let Some(tcp_sender) = self._tcp_sender.take() {
            tcp_sender.thread().unpark();
        }
    }
}
