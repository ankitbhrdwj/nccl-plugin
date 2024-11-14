//TODO: Implement the following functions
//
// pub fn tpa_memsegs_get() -> *mut tpa_memseg;
// pub fn tpa_extmem_register
// pub fn tpa_extmem_unregister

use crate::interface::SocketHandle;
use crate::utils;
use anyhow::Error;
use libtcp::ffi::{
    tpa_event, tpa_iovec, tpa_ip, tpa_worker, tpa_zreadv, TPA_EVENT_ERR, TPA_EVENT_HUP,
    TPA_EVENT_IN, TPA_EVENT_OUT,
};
use nix::sys::socket::{IpAddr, Ipv4Addr, SockAddr};
use std::fs::File;
use std::io::Write;
use std::mem::MaybeUninit;
use std::ptr;

pub fn get_numa_node_count() -> usize {
    let path = "/sys/devices/system/node";
    let mut numa_node_count = 0;

    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries {
            if let Ok(entry) = entry {
                if entry.file_name().to_str().unwrap_or("").starts_with("node") {
                    numa_node_count += 1;
                }
            }
        }
    }
    numa_node_count
}

// Don't change this function without checking
// https://github.com/bytedance/libtpa/blob/main/doc/user_guide.rst#config-options
pub fn libtcp_config(device: &utils::NCCLSocketDev) -> Result<(), Error> {
    let mut file = File::create("tpa.cfg").unwrap();
    let ip = match device.addr {
        SockAddr::Inet(inet_addr) => match inet_addr.ip() {
            IpAddr::V4(ipv4) => ipv4,
            _ => {
                return Err(Error::msg("Invalid socket address"));
            }
        },
        _ => return Err(Error::msg("Invalid socket address")),
    };
    let octets = ip.octets();
    let gw = IpAddr::V4(Ipv4Addr::new(octets[0], octets[1], octets[2], 0));

    file.write_all(b"net { ").unwrap();
    file.write_all(format!("name = {}; ", device.interface_name).as_bytes())
        .unwrap();
    file.write_all(format!("ip = {}; ", ip).as_bytes()).unwrap();
    file.write_all(format!("gw = {}; ", gw).as_bytes()).unwrap();
    file.write_all(b"mask = 255.255.255.0; }\n").unwrap();
    match get_numa_node_count() {
        1 => file.write_all(
            format!("dpdk {{ pci = {}; socket-mem = 8192; }}\n", device.pci_path).as_bytes(),
        ),
        2 => file.write_all(
            format!(
                "dpdk {{ pci = {}; socket-mem = 8192,8192; numa = 1; }}\n",
                device.pci_path
            )
            .as_bytes(),
        ),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Invalid numa node count",
        )),
    }
    .unwrap();
    file.write_all(b"tcp { snd_queue_size = 2048; tso = 1; opt_seq = 1; usr_snd_mss = 8832; opt_sack = 1;  }\n")
        .unwrap();
    file.write_all(b"trace { enable = 0; }\n").unwrap();
    file.flush().unwrap();
    Ok(())
}

// TCP worker related functions
pub fn tcp_init(nr_worker: i32) -> Result<(), std::io::Error> {
    match unsafe { libtcp::ffi::tpa_init(nr_worker) } {
        0 => Ok(()),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "tpa_init failed",
        )),
    }
}

pub fn tcp_worker_init() -> Box<tpa_worker> {
    match unsafe { libtcp::ffi::tpa_worker_init() } {
        ptr if ptr.is_null() => panic!("tpa_worker_init failed"),
        ptr => unsafe { Box::from_raw(ptr) },
    }
}

pub fn tcp_worker_run(worker: &mut Box<tpa_worker>) {
    unsafe { libtcp::ffi::tpa_worker_run(worker.as_mut()) }
}

// TCP connection related functions
pub fn tcp_connect_to(
    server: String,
    port: u16,
    opts: Option<libtcp::ffi::tpa_sock_opts>,
) -> Result<i32, std::io::Error> {
    let server = std::ffi::CString::new(server).unwrap();
    match unsafe {
        libtcp::ffi::tpa_connect_to(
            server.as_bytes().as_ptr() as *const i8,
            port,
            opts.map_or(std::ptr::null(), |opts| &opts),
        )
    } {
        sid if sid < 0 => Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "tpa_connect_to failed",
        )),
        sid => Ok(sid),
    }
}

pub fn tcp_listen_on(
    local_ip: String,
    port: u16,
    opts: Option<libtcp::ffi::tpa_sock_opts>,
) -> Result<(), std::io::Error> {
    let local = std::ffi::CString::new(local_ip).unwrap();
    let ptr = if local.is_empty() {
        std::ptr::null()
    } else {
        local.as_bytes().as_ptr() as *const i8
    };

    match unsafe {
        libtcp::ffi::tpa_listen_on(ptr, port, opts.map_or(std::ptr::null(), |opts| &opts))
    } {
        ret if ret < 0 => Err(std::io::Error::new(
            std::io::Error::from_raw_os_error(ret).kind(),
            "tpa_listen_on failed",
        )),
        _ => Ok(()),
    }
}

pub fn tcp_listen(
    socket_handle: SocketHandle,
    opts: Option<libtcp::ffi::tpa_sock_opts>,
) -> Result<(), std::io::Error> {
    let ip = socket_handle.ipv4().expect("Invalid socket address");
    let port = socket_handle.port().expect("Invalid socket address");
    tcp_listen_on(ip.to_string(), port, opts).expect("tcp_listen_on failed");
    Ok(())
}

pub fn tcp_accept(worker: &mut Box<tpa_worker>) -> Result<i32, std::io::Error> {
    let mut sid = -1;

    loop {
        tcp_worker_run(worker);
        if tcp_accept_burst(worker, &mut sid) == 1 {
            tcp_event_register(sid, TPA_EVENT_IN).expect("tcp_event_register failed");
            let _info = tcp_socket_info_get(sid).expect("tcp_socket_info_get failed");
            /*
            println!(
                "Local IP {}, Local port {}, remote IP {}, remote port {}",
                tpa_ip_to_str(_info.local_ip),
                _info.local_port,
                tpa_ip_to_str(_info.remote_ip),
                _info.remote_port
            );
            */
            break;
        }
    }
    Ok(sid)
}

pub fn tcp_connect(
    worker: &mut Box<tpa_worker>,
    socket_handle: SocketHandle,
    opts: Option<libtcp::ffi::tpa_sock_opts>,
) -> Result<i32, std::io::Error> {
    let ip = socket_handle.ipv4().expect("Invalid socket address");
    let port = socket_handle.port().expect("Invalid socket address");
    let fd = tcp_connect_to(ip.to_string(), port, opts).expect("tcp_connect_to failed");
    tcp_event_register(
        fd,
        TPA_EVENT_IN | TPA_EVENT_OUT | TPA_EVENT_ERR | TPA_EVENT_HUP,
    )
    .expect("tcp_event_register failed");
    let mut uninit = [MaybeUninit::<tpa_event>::uninit(); 1];
    let mut events = uninit
        .iter_mut()
        .map(|x| unsafe { x.assume_init() })
        .collect::<Vec<tpa_event>>();
    while tcp_event_poll(worker, &mut events, 1) == 0 {}
    Ok(fd)
}

pub fn tcp_socket_info_get(sid: i32) -> Result<libtcp::ffi::tpa_sock_info, std::io::Error> {
    let mut uninit = MaybeUninit::<libtcp::ffi::tpa_sock_info>::uninit();
    let info = uninit.as_mut_ptr();
    match unsafe { libtcp::ffi::tpa_sock_info_get(sid, info) } {
        0 => Ok(unsafe { uninit.assume_init() }),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "tpa_sock_info_get failed",
        )),
    }
}

pub fn tcp_accept_burst(worker: &mut Box<tpa_worker>, sid: &mut i32) -> i32 {
    unsafe { libtcp::ffi::tpa_accept_burst(worker.as_mut(), sid, 1) }
}

#[allow(dead_code)]
pub fn tpa_ip_to_str(ip: tpa_ip) -> String {
    unsafe { std::net::Ipv4Addr::from_bits(ip.__bindgen_anon_1.u32_[3]).to_string() }
}

// Event related functions
pub fn tcp_event_register(sid: i32, events: u32) -> Result<(), std::io::Error> {
    let mut uninit = MaybeUninit::<tpa_event>::uninit();
    let event = uninit.as_mut_ptr();
    let mut event = unsafe {
        (*event).events = events;
        (*event).data = sid as *mut std::ffi::c_void;
        uninit.assume_init()
    };

    match unsafe {
        libtcp::ffi::tpa_event_ctrl(sid, libtcp::ffi::TPA_EVENT_CTRL_ADD as i32, &mut event)
    } {
        0 => Ok(()),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "tpa_event_ctrl failed",
        )),
    }
}

pub fn tcp_event_poll(
    worker: &mut Box<tpa_worker>,
    events: &mut [tpa_event],
    maxevents: i32,
) -> i32 {
    tcp_worker_run(worker);
    // assert!(events.len() as i32 >= maxevents);
    unsafe { libtcp::ffi::tpa_event_poll(worker.as_mut(), events.as_mut_ptr(), maxevents) }
}

pub struct TCPReader {
    fd: i32,
    events: Vec<tpa_event>,
    iov: tpa_iovec,
    partial: Option<Vec<u8>>,
}

impl TCPReader {
    pub fn new(worker: &mut Box<tpa_worker>) -> Self {
        let fd = tcp_accept(worker).expect("tcp_listen_and_accept failed");
        let mut uninit = [MaybeUninit::<tpa_event>::uninit(); 32];
        let events = uninit
            .iter_mut()
            .map(|x| unsafe { x.assume_init() })
            .collect::<Vec<tpa_event>>();

        let uninit = MaybeUninit::<tpa_iovec>::uninit();
        let iov = unsafe { uninit.assume_init() };
        TCPReader {
            fd,
            partial: None,
            events,
            iov,
        }
    }

    pub fn read_exact(
        &mut self,
        worker: &mut Box<tpa_worker>,
        buf: &mut [u8],
        size: usize,
    ) -> Result<isize, std::io::Error> {
        // assert!(buf.len() >= size);
        let mut buf = &mut buf[..size];
        if self.partial.is_some() {
            let partial = self.partial.take().unwrap();
            let len = std::cmp::min(partial.len(), buf.len());
            buf[..len].copy_from_slice(&partial[..len]);
            if len < partial.len() {
                self.partial = Some(partial[len..].to_vec());
                return Ok(len as isize);
            }
            buf = &mut buf[len..];
        }

        while !buf.is_empty() {
            self.iov.iov_len = 0;
            let ret = unsafe { tpa_zreadv(self.fd, &mut self.iov, 1) };

            if ret > 0 {
                let src = unsafe {
                    std::slice::from_raw_parts(
                        self.iov.iov_base as *const u8,
                        self.iov.iov_len as usize,
                    )
                };
                let tmp = buf;
                if src.len() <= tmp.len() {
                    tmp[..src.len()].copy_from_slice(src);
                    buf = &mut tmp[src.len()..];
                } else {
                    tmp.copy_from_slice(&src[..tmp.len()]);
                    self.partial = Some(src[tmp.len()..].to_vec());
                    buf = &mut tmp[0..0];
                }
                unsafe {
                    self.iov.__bindgen_anon_1.iov_read_done.unwrap()(
                        self.iov.iov_base,
                        self.iov.iov_param,
                    )
                };
            } else {
                tcp_event_poll(worker, &mut self.events, 32);
            }
        }

        Ok(size as isize)
    }
}

impl Drop for TCPReader {
    fn drop(&mut self) {
        unsafe { libtcp::ffi::tpa_close(self.fd) };
    }
}

pub struct TCPWriter {
    fd: i32,
    events: Vec<tpa_event>,
    iov: tpa_iovec,
}

impl TCPWriter {
    pub fn new(
        worker: &mut Box<tpa_worker>,
        socket_handle: SocketHandle,
        opts: Option<libtcp::ffi::tpa_sock_opts>,
    ) -> Self {
        let fd = tcp_connect(worker, socket_handle.clone(), opts).expect("tcp_connect failed");
        let mut uninit = [MaybeUninit::<tpa_event>::uninit(); 32];
        let events = uninit
            .iter_mut()
            .map(|x| unsafe { x.assume_init() })
            .collect::<Vec<tpa_event>>();

        let uninit = MaybeUninit::<tpa_iovec>::uninit();
        let iov = unsafe { uninit.assume_init() };
        TCPWriter { events, iov, fd }
    }

    pub fn tcp_write(
        &mut self,
        worker: &mut Box<tpa_worker>,
        buf: &[u8],
        opt_dscp: u8,
        opt_seq: u32,
    ) -> Result<isize, std::io::Error> {
        self.iov.iov_base = buf.as_ptr() as *mut std::ffi::c_void;
        self.iov.iov_len = buf.len() as u32;
        self.iov.iov_phys = 0;
        self.iov.__bindgen_anon_1.iov_write_done = None;
        self.iov.iov_param = ptr::null_mut();
        // Not in network order
        self.iov.dscp_bits = opt_dscp;
        self.iov.optional_seq = opt_seq;

        loop {
            tcp_worker_run(worker);
            let ret = unsafe { libtcp::ffi::tpa_zwritev(self.fd, &self.iov, 1) };
            if ret < 0 {
                continue;
            }

            tcp_event_poll(worker, &mut self.events, 32);
            return Ok(ret);
        }
    }
}

impl Drop for TCPWriter {
    fn drop(&mut self) {
        unsafe { libtcp::ffi::tpa_close(self.fd) };
    }
}
