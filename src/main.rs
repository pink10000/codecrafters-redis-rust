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

use parser::RespType;
use server::{ServerAddr, ServerState};

const DEFAULT_PORT: u16 = 6379;

fn handle_client(mut stream: TcpStream, srv: &Arc<Mutex<ServerState>>) {
    loop {
        let mut buf: [u8; 1024] = [0; 1024];
        let read_res: Result<usize, Error> = stream.read(&mut buf);
        let resp: parser::RespType;

        match read_res {
            Ok(0) => {
                println!("Connection closed\n");
                return;
            }
            Ok(size) => {
                let command = String::from_utf8_lossy(&buf[..size]);
                resp = parser::parse_resp(&command).unwrap();
                println!("Received command: {:?}", resp);
            }
            Err(e) => {
                eprintln!("Failed to read from stream: {}", e);
                return;
            }
        }

        // before it parses the response and and changes the state of the server
        // it needs to lock the server state, so that no other thread can access it
        let parsed_response: RespType = srv.lock().unwrap().execute_resp(resp);
        let serialized_response: String = parsed_response.to_resp_string();
        let _ = stream.write(serialized_response.as_bytes());
        println!("Sent response: {:?}", serialized_response);
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

    // server state that is shared between threads
    let server_state = Arc::new(Mutex::new(ServerState::new(port, replica_of)));
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).unwrap();

    for stream in listener.incoming() {
        println!("Found stream, handling connection:");
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
