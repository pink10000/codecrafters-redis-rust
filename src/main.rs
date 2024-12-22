#![allow(unused_imports)]
pub mod parser;
pub mod server;

use std::{
    env,
    io::{Error, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
};

use parser::{parse_retain_cmd, RespType};
use server::{ServerAddr, ServerState};

const DEFAULT_PORT: u16 = 6379;

fn handle_client(mut stream: TcpStream, srv: &Arc<Mutex<ServerState>>) {
    let role: String = srv.lock().unwrap().get_role();
    loop {
        let mut buf: [u8; 1024] = [0; 1024];
        let read_res: Result<usize, Error> = stream.read(&mut buf);
        let resp: parser::RespType;

        match read_res {
            Ok(0) => {
                return;
            }
            Ok(size) => {
                let command = String::from_utf8_lossy(&buf[..size]);
                resp = parser::parse_resp(&command).unwrap();
                println!("{} received command: {:?}", role, resp);
            }
            Err(e) => {
                eprintln!("Failed to read from stream: {}", e);
                return;
            }
        }

        // before it parses the response and and changes the state of the server
        // it needs to lock the server state, so that no other thread can access it
        let parsed_response: RespType = srv.lock().unwrap().execute_resp(resp.clone());
        let serialized_response: String = parsed_response.to_resp_string();
        let _ = stream.write(serialized_response.as_bytes());
        println!("-Sent response: {:?}", serialized_response);

        // check if resp has a slave of command; if it does, extract it 
        // this is a bad way to do it.... idk how else to do it
        if parse_retain_cmd(&resp.clone()) {
            match stream.try_clone() {
                Ok(cloned_stream) => {
                    srv.lock().unwrap().retain_slave(cloned_stream);
                }
                Err(e) => {
                    eprintln!("Failed to clone stream: {}", e);
                }
            }
        }

        // check if resp needs to do a full resync (check for full resync command)
        // if it does, then send it after the serialized response
        // this is a bad way to do it.... idk how else to do it
        if serialized_response.contains("FULLRESYNC") {
            let full_resync: (String, Vec<u8>) = srv.lock().unwrap().full_resync();
            let _ = stream.write(full_resync.0.as_bytes());
            let _ = stream.write(full_resync.1.as_slice());
        }
    }
}

fn main() {
    let mut port: u16 = DEFAULT_PORT;
    let mut replica_of: Option<ServerAddr> = None;

    let args: Vec<String> = env::args().collect();

    let mut idx: usize = 1; // needs to be one to skip the binary call
    while idx < args.len() {
        let arg = &args[idx as usize];
        match arg.as_str() {
            "--port" => {
                if args.len() > (idx as usize) + 1 {
                    port = args[(idx + 1) as usize].parse::<u16>().unwrap();
                    idx += 1;
                } else {
                    eprintln!("Port number not provided");
                    return;
                }
            }
            "--replicaof" => {
                if args.len() > (idx as usize) + 1 {
                    // ip + port passed a singular string
                    let ip_port = args[(idx + 1) as usize].clone();
                    let mut split = ip_port.split(" ");
                    let ip: String = split.next().unwrap().to_string();
                    let port: u16 = split.next().unwrap().parse::<u16>().unwrap();
                    replica_of = Some(ServerAddr::new(ip, port));
                    idx += 2;
                } else {
                    eprintln!("Replicaof requires ip and port");
                    return;
                }
            }
            _ => {
                eprintln!("Unknown flag: {}", arg);
                return;
            }
        }
        idx += 1;
    }

    let server_state: ServerState = ServerState::new(port, replica_of);
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).unwrap();
    
    // needs to request replication if the server is a slave
    let srv_role: String = server_state.get_role();
    println!("Server role: {}\n", srv_role);
    if srv_role == "slave" {
        println!("-Requesting replication...");
        server_state.request_replication();
        println!("-Replication finished.");
    }
    
    // server state that is shared between threads
    let server_state = Arc::new(Mutex::new(server_state.clone()));
    for stream in listener.incoming() {
        println!("\nFound stream, handling connection:");
        match stream {
            Ok(stream) => {
                thread::spawn({
                    let srv = Arc::clone(&server_state);
                    move || {
                        handle_client(stream, &srv);
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}