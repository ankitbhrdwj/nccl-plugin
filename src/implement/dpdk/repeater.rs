use super::tcp::TCPWriter;
use crate::interface::SocketHandle;
use libtcp::ffi::tpa_worker;
use serde::{Deserialize, Serialize};

pub struct Repeater {
    ctrl_writer: Option<TCPWriter>,
    data_writer: Option<TCPWriter>,
}

#[derive(Serialize, Deserialize)]
pub struct ChunkInfo {
    pub chunk_offset: i32,
    pub chunk_size: i32,
}

impl Repeater {
    pub fn new(
        rank: i32,
        nranks: i32,
        _worker: &mut Box<tpa_worker>,
        _socket: SocketHandle,
    ) -> Self {
        let (ctrl_writer, data_writer) = if rank == 0 || (rank == nranks - 1) {
            #[cfg(feature = "storage")]
            {
                (
                    Some(TCPWriter::new(_worker, _socket.clone(), None)),
                    Some(TCPWriter::new(_worker, _socket.clone(), None)),
                )
            }
            #[cfg(not(feature = "storage"))]
            (None, None)
        } else {
            (None, None)
        };
        Self {
            ctrl_writer,
            data_writer,
        }
    }

    pub fn repeat_tcp_write(
        &mut self,
        worker: &mut Box<tpa_worker>,
        chunk_offset: i32,
        buf: &[u8],
    ) {
        if self.ctrl_writer.is_none() || self.data_writer.is_none() {
            return;
        }

        let chunk_info = ChunkInfo {
            chunk_offset,
            chunk_size: buf.len() as i32,
        };
        let info_bytes = bincode::serialize(&chunk_info).unwrap();
        self.ctrl_writer
            .as_mut()
            .unwrap()
            .tcp_write(worker, &info_bytes)
            .expect("failed to write chunk info");

        self.data_writer
            .as_mut()
            .unwrap()
            .tcp_write(worker, buf)
            .expect("failed to write data");
        super::tcp::tcp_worker_run(worker);
    }
}
