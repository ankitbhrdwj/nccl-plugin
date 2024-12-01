use crate::interface::{
    BaguaNetError, NCCLNetProperties, Net, SocketHandle, SocketListenCommID, SocketRecvCommID,
    SocketRequestID, SocketSendCommID,
};
use crate::utils;
use nix::sched::{sched_setaffinity, CpuSet};
use nix::sys::socket::{InetAddr, IpAddr, SockAddr};
use nix::unistd::Pid;
use std::collections::HashMap;
use std::str::FromStr;

use thread_priority::*;

use std::sync::{Arc, Barrier};
use std::sync::{Mutex, RwLock};

use super::tcp::*;
use super::{ffi::*, Repeater};

const WARMPUP_BUCKETID: i32 = 0;

enum NcclPtr {
    HostPtr = 1,
    _CudaPtr = 2,
}

pub fn set_affinity(coreid: usize) {
    let mut cpu_set = CpuSet::new();
    cpu_set.set(coreid).unwrap();
    sched_setaffinity(Pid::from_raw(0), &cpu_set).unwrap();
    set_current_thread_priority(ThreadPriority::Max).unwrap();
    unsafe {tpa_thread_register();}
    /*
    let thread_id = thread_native_id();
assert!(set_thread_priority_and_policy(thread_id,
                                       ThreadPriority::Max,
                                       ThreadSchedulePolicy::Realtime(RealtimeThreadSchedulePolicy::Fifo)).is_ok());
    */
}

#[derive(Debug)]
pub struct RequestState {
    pub nsubtasks: usize,
    pub completed_subtasks: usize,
    pub nbytes_transferred: usize,
    pub bucket_id: i32,
    pub chunk_tag: i32,
    pub err: Option<BaguaNetError>,
}

#[derive(Clone)]
pub struct SocketSendComm {
    pub sid: i32,
    pub _tcp_sender: Arc<std::thread::JoinHandle<()>>,
    pub msg_sender: flume::Sender<(&'static [u8], Arc<RwLock<RequestState>>)>,
    pub _repeater: Repeater,
}

#[derive(Clone)]
pub struct SocketRecvComm {
    pub sid: i32,
    pub _tcp_reciever: Arc<std::thread::JoinHandle<()>>,
    pub msg_sender: flume::Sender<(&'static mut [u8], Arc<Mutex<RequestState>>)>,
}

pub struct SocketSendRequest {
    pub state: Arc<RwLock<RequestState>>,
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
    pub nccl_nchannels: i32,
    pub start_listen_port: u16,
    devices: Vec<utils::NCCLSocketDev>,
    pub listen_comm_next_id: usize,
    pub listen_comm_map: HashMap<SocketListenCommID, Arc<Barrier>>,
    pub send_comm_next_id: usize,
    pub send_comm_map: HashMap<SocketSendCommID, SocketSendComm>,
    pub recv_comm_next_id: usize,
    pub recv_comm_map: HashMap<SocketRecvCommID, SocketRecvComm>,
    pub socket_request_next_id: usize,
    pub socket_request_map: HashMap<SocketRequestID, SocketRequest>,
    pub num_storage: u8,

    // These are for explicit TCP connections to storage servers.
    pub storage_server_ip: IpAddr,
    pub storage_server_port: u16,
}

impl BaguaNet {
    const DEFAULT_SOCKET_MAX_COMMS: i32 = 65536;
    const NR_WORKERS: i32 = 32;

    pub fn new() -> Result<BaguaNet, BaguaNetError> {
        let rank: i32 = std::env::var("RANK")
            .unwrap_or("-1".to_string())
            .parse()
            .unwrap();
        let nranks: i32 = std::env::var("WORLD_SIZE")
            .unwrap_or("-1".to_string())
            .parse()
            .unwrap();
        let start_listen_port = std::env::var("START_LISTEN_PORT")
            .unwrap_or("41000".to_string())
            .parse()
            .unwrap();
        let storage_ip: String =
            std::env::var("STORAGE_SERVER_IP").unwrap_or("10.40.1.104".to_string());
        let storage_ip = std::net::Ipv4Addr::from_str(&storage_ip).unwrap().octets();

        let num_storage: u8 = std::env::var("NUM_STORAGE")
            .unwrap_or("1".to_string())
            .parse()
            .unwrap();
        assert!(num_storage <= 8);

        let nccl_nchannels: i32 = std::env::var("NCCL_MAX_NCHANNELS")
            .unwrap_or("4".to_string())
            .parse()
            .unwrap();
        assert!(BaguaNet::NR_WORKERS >= 2 * nccl_nchannels);

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
            nccl_nchannels,
            start_listen_port,
            devices: utils::find_interfaces(),
            listen_comm_next_id: 0,
            listen_comm_map: Default::default(),
            send_comm_next_id: 0,
            send_comm_map: Default::default(),
            recv_comm_next_id: 0,
            recv_comm_map: Default::default(),
            socket_request_next_id: 0,
            socket_request_map: Default::default(),
            num_storage,
            storage_server_ip: IpAddr::new_v4(
                storage_ip[0],
                storage_ip[1],
                storage_ip[2],
                storage_ip[3],
            ),
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

        // To sync connection establishment and nccl proxy to actually accept
        // connections.
        let barrier = Arc::new(Barrier::new(2));
        self.listen_comm_map.insert(listen_id, barrier.clone());

        // To sync worker to be ready before sending port to nccl, which I think
        // is used by nccl to send to the other workers as metadata for
        // connection.
        let worker_ready = Arc::new(Barrier::new(2));

        let id = self.recv_comm_next_id;
        self.recv_comm_next_id += 1;
        let port = self.start_listen_port + id as u16 + (self.rank * self.nccl_nchannels) as u16;
        println!("Rank {} worker {} is listening on {}", self.rank, id, port);
        let socket_handle = SocketHandle {
            addr: SockAddr::new_inet(InetAddr::new(addr.ip(), port)),
        };
        let handle = socket_handle.clone();

        let (msg_sender, msg_receiver) = flume::unbounded();
        let b = barrier.clone();
        let worker_b = worker_ready.clone();
        self.recv_comm_map.insert(
            id,
            SocketRecvComm {
                sid: 0,
                msg_sender,
                _tcp_reciever: Arc::new(std::thread::spawn(move || {
                    let mut worker = tcp_worker_init();
                    set_affinity((id + 8) % 16);
                    tcp_listen(socket_handle, None).expect("tcp_listen failed");
                    worker_b.wait();

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

        worker_ready.wait();
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

        let (msg_repeater_sender, msg_repeater_receiver) = flume::unbounded();
        let (msg_sender, msg_receiver) = flume::unbounded();

        let _repeater = Repeater::new(
            self.rank,
            self.nranks,
            msg_repeater_receiver,
            storage_server_handle.clone(),
        );

        let num_storage = self.num_storage;
        self.send_comm_map.insert(
            id,
            SocketSendComm {
                sid: 0,
                msg_sender,
                _repeater,
                _tcp_sender: Arc::new(std::thread::spawn(move || {
                    let mut worker = tcp_worker_init();
                    set_affinity(id + 16);
                    let mut data_writer = TCPWriter::new(&mut worker, socket_handle.clone(), None);
                    let mut ctrl_writer = TCPWriter::new(&mut worker, socket_handle, None);

                    // Make it one so that toggle for Bucket 0 makes it zero.
                    let mut sid = 0u8;
                    let mut current_active_bucket = -1;

                    let mut next_reduced_byte_offsets = vec![1u32; num_storage as usize];
                    let mut reduced_byte_offsets = vec![1u32; num_storage as usize];

                    // Sender loop
                    loop {
                        tcp_worker_run(&mut worker);
                        if let Ok((data, state)) = msg_receiver.try_recv() {
                            let chunk_offset = state.read().unwrap().chunk_tag;
                            let bucket_id = state.read().unwrap().bucket_id;

                            // Bit 0 and 1: ECN, Bit 2 - 6: Storage ID, Bit 7: Tagged
                            let mut dscp_bits = 0u8;
                            if bucket_id != WARMPUP_BUCKETID {
                                match bucket_id {
                                    1 => {
                                        sid = 0;
                                        current_active_bucket = bucket_id;
                                    }
                                    _ => {
                                        if current_active_bucket != bucket_id {
                                            sid = (sid + 1) % num_storage;
                                            current_active_bucket = bucket_id;
                                        }
                                    }
                                }

                                if !chunk_offset.is_negative() {
                                    dscp_bits |= sid << 2; // Bit 2 - 6: Storage ID
                                    dscp_bits |= 0x80; // Bit 7: set if it's tagged
                                    next_reduced_byte_offsets[sid as usize] =
                                        reduced_byte_offsets[sid as usize] + data.len() as u32;
                                }
                            }

                            let send_nbytes = data.len().to_be_bytes();
                            ctrl_writer
                                .tcp_write(&mut worker, &send_nbytes, 0, 0)
                                .expect("tcp_write failed");
                            data_writer
                                .tcp_write(
                                    &mut worker,
                                    data,
                                    dscp_bits,
                                    reduced_byte_offsets[sid as usize],
                                )
                                .expect("tcp_write failed");

                            reduced_byte_offsets[sid as usize] =
                                next_reduced_byte_offsets[sid as usize];

                            // Storage write only works when "storage" feature
                            // is enabled; otherwise it's a noop.
                            if cfg!(feature = "storage")
                                && !chunk_offset.is_negative()
                                && bucket_id != WARMPUP_BUCKETID
                            {
                                msg_repeater_sender
                                    .send((chunk_offset, data, state.clone()))
                                    .unwrap();
                            }

                            match state.write() {
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
        bucket_id: i32,
        chunk_tag: i32,
    ) -> Result<SocketRequestID, BaguaNetError> {
        let request_id = self.socket_request_next_id;
        self.socket_request_next_id += 1;
        let send_comm = self.send_comm_map.get(&send_comm_id).unwrap();
        let mut nsubtasks = 1;
        if cfg!(feature = "storage") && !chunk_tag.is_negative() && bucket_id != WARMPUP_BUCKETID {
            nsubtasks = 2;
        }
        let task_state = Arc::new(RwLock::new(RequestState {
            nsubtasks,
            completed_subtasks: 0,
            nbytes_transferred: 0,
            bucket_id,
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
            bucket_id: 0,
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
                let state = send_req.state.read().unwrap();
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
