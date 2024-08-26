use crate::interface::{
    BaguaNetError, NCCLNetProperties, Net, SocketHandle, SocketListenCommID, SocketRecvCommID,
    SocketRequestID, SocketSendCommID,
};
use crate::utils;
use nix::sys::socket::{InetAddr, IpAddr, SockAddr};
use std::collections::HashMap;

use rand::Rng;
use std::sync::Mutex;
use std::sync::{Arc, Barrier};

use super::tcp::*;
use super::{ffi::*, Repeater};

enum NcclPtr {
    HostPtr = 1,
    CudaPtr = 2,
}

#[derive(Debug)]
pub struct RequestState {
    pub nsubtasks: usize,
    pub completed_subtasks: usize,
    pub nbytes_transferred: usize,
    pub chunk_tag: i32,
    pub err: Option<BaguaNetError>,
}

#[derive(Clone)]
pub struct SocketSendComm {
    pub sid: i32,
    pub _tcp_sender: Arc<std::thread::JoinHandle<()>>,
    pub msg_sender: flume::Sender<(&'static [u8], Arc<Mutex<RequestState>>)>,
}

#[derive(Clone)]
pub struct SocketRecvComm {
    pub sid: i32,
    pub _tcp_reciever: Arc<std::thread::JoinHandle<()>>,
    pub msg_sender: flume::Sender<(&'static mut [u8], Arc<Mutex<RequestState>>)>,
}

pub struct SocketSendRequest {
    pub state: Arc<Mutex<RequestState>>,
}

pub struct SocketRecvRequest {
    pub state: Arc<Mutex<RequestState>>,
}

pub enum SocketRequest {
    SendRequest(SocketSendRequest),
    RecvRequest(SocketRecvRequest),
}

pub struct BaguaNet {
    pub rank: i32,
    pub nranks: i32,
    devices: Vec<utils::NCCLSocketDev>,
    pub listen_comm_next_id: usize,
    pub listen_comm_map: HashMap<SocketListenCommID, Arc<Barrier>>,
    pub send_comm_next_id: usize,
    pub send_comm_map: HashMap<SocketSendCommID, SocketSendComm>,
    pub recv_comm_next_id: usize,
    pub recv_comm_map: HashMap<SocketRecvCommID, SocketRecvComm>,
    pub socket_request_next_id: usize,
    pub socket_request_map: HashMap<SocketRequestID, SocketRequest>,

    pub storage_server_ip: IpAddr,
    pub storage_server_port: u16,
}

impl BaguaNet {
    const DEFAULT_SOCKET_MAX_COMMS: i32 = 65536;
    const NR_WORKERS: i32 = 8;

    pub fn new() -> Result<BaguaNet, BaguaNetError> {
        let rank: i32 = std::env::var("RANK")
            .unwrap_or("-1".to_string())
            .parse()
            .unwrap();
        let nranks: i32 = std::env::var("WORLD_SIZE")
            .unwrap_or("-1".to_string())
            .parse()
            .unwrap();

        let devices = utils::find_interfaces();
        if devices.is_empty() {
            return Err(BaguaNetError::InnerError(
                "No available network devices found".to_string(),
            ));
        }
        libtcp_config(&devices[0]).expect("network_config failed");
        tcp_init(BaguaNet::NR_WORKERS).expect("tcp_init failed");

        Ok(BaguaNet {
            rank,
            nranks,
            devices: utils::find_interfaces(),
            listen_comm_next_id: 0,
            listen_comm_map: Default::default(),
            send_comm_next_id: 0,
            send_comm_map: Default::default(),
            recv_comm_next_id: 0,
            recv_comm_map: Default::default(),
            socket_request_next_id: 0,
            socket_request_map: Default::default(),
            storage_server_ip: IpAddr::new_v4(10, 2, 1, 28),
            storage_server_port: if rank == 0 { 5678 } else { 5682 }, // Offset by 4, for 4 channels.
        })
    }
}

impl Net for BaguaNet {
    fn devices(&self) -> Result<usize, BaguaNetError> {
        Ok(self.devices.len())
    }

    fn get_properties(&self, dev_id: usize) -> Result<NCCLNetProperties, BaguaNetError> {
        let socket_dev = &self.devices[dev_id];

        let p = NCCLNetProperties {
            name: socket_dev.interface_name.clone(),
            pci_path: socket_dev.pci_path.clone(),
            guid: dev_id as u64,
            ptr_support: NcclPtr::HostPtr as i32,
            speed: utils::get_net_if_speed(&socket_dev.interface_name),
            port: 0,
            max_comms: BaguaNet::DEFAULT_SOCKET_MAX_COMMS,
        };
        Ok(p)
    }

    fn listen(
        &mut self,
        dev_id: usize,
    ) -> Result<(SocketHandle, SocketListenCommID), BaguaNetError> {
        let socket_dev = &self.devices[dev_id];
        let addr = match socket_dev.addr {
            SockAddr::Inet(inet_addr) => inet_addr,
            others => {
                return Err(BaguaNetError::InnerError(format!(
                    "Got invalid socket address, which is {:?}",
                    others
                )))
            }
        };

        let listen_id = self.listen_comm_next_id;
        self.listen_comm_next_id += 1;
        let barrier = Arc::new(Barrier::new(2));
        self.listen_comm_map.insert(listen_id, barrier.clone());

        let id = self.recv_comm_next_id;
        self.recv_comm_next_id += 1;
        let port = (rand::thread_rng().gen_range(41000..64000) as u16).to_be();
        let socket_handle = SocketHandle {
            addr: SockAddr::new_inet(InetAddr::new(addr.ip(), port)),
        };
        let handle = socket_handle.clone();

        let (msg_sender, msg_receiver) = flume::unbounded();
        let b = barrier.clone();
        self.recv_comm_map.insert(
            id,
            SocketRecvComm {
                sid: 0,
                msg_sender,
                _tcp_reciever: Arc::new(std::thread::spawn(move || {
                    let mut worker = tcp_worker_init();
                    tcp_listen(socket_handle, None).expect("tcp_listen failed");
                    let mut data_reader = TCPReader::new(&mut worker);
                    let mut ctrl_reader = TCPReader::new(&mut worker);
                    b.wait();

                    // Reciever loop
                    loop {
                        tcp_worker_run(&mut worker);

                        if let Ok((data, state)) = msg_receiver.try_recv() {
                            let mut target_nbytes = data.len().to_be_bytes();
                            let size = target_nbytes.len();
                            ctrl_reader
                                .read_exact(&mut worker, &mut target_nbytes[..], size)
                                .unwrap();
                            let target_nbytes = usize::from_be_bytes(target_nbytes);
                            data_reader
                                .read_exact(&mut worker, data, target_nbytes)
                                .unwrap();
                            match state.lock() {
                                Ok(mut state) => {
                                    state.completed_subtasks += 1;
                                    state.nbytes_transferred += target_nbytes;
                                }
                                Err(poisoned) => {
                                    tracing::warn!("{:?}", poisoned);
                                }
                            };
                        }
                    }
                })),
            },
        );

        Ok((handle, id))
    }

    fn connect(
        &mut self,
        _dev_id: usize,
        socket_handle: SocketHandle,
    ) -> Result<SocketSendCommID, BaguaNetError> {
        let id = self.send_comm_next_id;
        self.send_comm_next_id += 1;

        let storage_server_handle = SocketHandle {
            addr: SockAddr::new_inet(InetAddr::new(
                self.storage_server_ip,
                self.storage_server_port + id as u16,
            )),
        };

        let (msg_sender, msg_receiver) = flume::unbounded();
        let rank = self.rank;
        let nranks = self.nranks;
        self.send_comm_map.insert(
            id,
            SocketSendComm {
                sid: 0,
                msg_sender,
                _tcp_sender: Arc::new(std::thread::spawn(move || {
                    let mut worker = tcp_worker_init();
                    let mut data_writer = TCPWriter::new(&mut worker, socket_handle.clone(), None);
                    let mut ctrl_writer = TCPWriter::new(&mut worker, socket_handle, None);
                    let mut storage_writer =
                        Repeater::new(rank, nranks, &mut worker, storage_server_handle);

                    // Sender loop
                    loop {
                        tcp_worker_run(&mut worker);
                        if let Ok((data, state)) = msg_receiver.try_recv() {
                            let send_nbytes = data.len().to_be_bytes();
                            ctrl_writer
                                .tcp_write(&mut worker, &send_nbytes)
                                .expect("tcp_write failed");
                            data_writer
                                .tcp_write(&mut worker, data)
                                .expect("tcp_write failed");

                            // Storage write only works when "storage" feature
                            // is enabled; otherwise it's a noop.
                            let chunk_offset = state.lock().unwrap().chunk_tag;
                            if !chunk_offset.is_negative() {
                                storage_writer.repeat_tcp_write(&mut worker, chunk_offset, data);
                            }

                            match state.lock() {
                                Ok(mut state) => {
                                    state.completed_subtasks += 1;
                                    state.nbytes_transferred += data.len();
                                }
                                Err(poisoned) => {
                                    tracing::warn!("{:?}", poisoned);
                                }
                            };
                        }
                    }
                })),
            },
        );

        Ok(id)
    }

    fn accept(
        &mut self,
        listen_comm_id: SocketListenCommID,
    ) -> Result<SocketRecvCommID, BaguaNetError> {
        let barrier = self.listen_comm_map.get(&listen_comm_id).unwrap();
        barrier.wait();
        Ok(listen_comm_id)
    }

    fn isend(
        &mut self,
        send_comm_id: SocketSendCommID,
        data: &'static [u8],
        chunk_tag: i32,
    ) -> Result<SocketRequestID, BaguaNetError> {
        let request_id = self.socket_request_next_id;
        self.socket_request_next_id += 1;
        let send_comm = self.send_comm_map.get(&send_comm_id).unwrap();
        let task_state = Arc::new(Mutex::new(RequestState {
            nsubtasks: 1,
            completed_subtasks: 0,
            nbytes_transferred: 0,
            chunk_tag,
            err: None,
        }));
        self.socket_request_map.insert(
            request_id,
            SocketRequest::SendRequest(SocketSendRequest {
                state: task_state.clone(),
            }),
        );

        send_comm.msg_sender.send((data, task_state)).unwrap();

        Ok(request_id)
    }

    fn irecv(
        &mut self,
        recv_comm_id: SocketRecvCommID,
        data: &'static mut [u8],
    ) -> Result<SocketRequestID, BaguaNetError> {
        let request_id = self.socket_request_next_id;
        self.socket_request_next_id += 1;

        let recv_comm = self.recv_comm_map.get(&recv_comm_id).unwrap();
        let task_state = Arc::new(Mutex::new(RequestState {
            nsubtasks: 1,
            completed_subtasks: 0,
            nbytes_transferred: 0,
            chunk_tag: 0,
            err: None,
        }));
        self.socket_request_map.insert(
            request_id,
            SocketRequest::RecvRequest(SocketRecvRequest {
                state: task_state.clone(),
            }),
        );

        recv_comm.msg_sender.send((data, task_state)).unwrap();

        Ok(request_id)
    }

    fn test(&mut self, request_id: SocketRequestID) -> Result<(bool, usize), BaguaNetError> {
        let request = self.socket_request_map.get_mut(&request_id).unwrap();
        let ret = match request {
            SocketRequest::SendRequest(send_req) => {
                let state = send_req.state.lock().unwrap();
                if let Some(err) = state.err.clone() {
                    return Err(err);
                }

                let task_completed = state.nsubtasks == state.completed_subtasks;
                Ok((task_completed, state.nbytes_transferred))
            }
            SocketRequest::RecvRequest(recv_req) => {
                let state = recv_req.state.lock().unwrap();
                if let Some(err) = state.err.clone() {
                    return Err(err);
                }

                let task_completed = state.nsubtasks == state.completed_subtasks;
                Ok((task_completed, state.nbytes_transferred))
            }
        };

        if let Ok(ret) = ret {
            if ret.0 {
                self.socket_request_map.remove(&request_id).unwrap();
            }
        }

        ret
    }

    fn close_send(&mut self, send_comm_id: SocketSendCommID) -> Result<(), BaguaNetError> {
        if let Some(comm) = self.send_comm_map.remove(&send_comm_id) {
            unsafe { tpa_close(comm.sid) };
        }

        Ok(())
    }

    fn close_recv(&mut self, recv_comm_id: SocketRecvCommID) -> Result<(), BaguaNetError> {
        if let Some(comm) = self.recv_comm_map.remove(&recv_comm_id) {
            unsafe { tpa_close(comm.sid) };
        }

        Ok(())
    }

    fn close_listen(&mut self, listen_comm_id: SocketListenCommID) -> Result<(), BaguaNetError> {
        self.listen_comm_map.remove(&listen_comm_id);

        Ok(())
    }
}
