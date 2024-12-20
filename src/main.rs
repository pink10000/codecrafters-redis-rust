#![allow(unused_imports)]
pub mod parser;
pub mod server;

use std::{
    env,
    io::{Error, Read, Write},
    net::{TcpListener, TcpStream},
    thread,
};

use parser::RespType;
use server::ServerState;

const DEFAULT_PORT: u16 = 6379;

fn handle_client(mut stream: TcpStream) {
    let mut srv = ServerState::new();

    while stream.peek(&mut [0; 1]).is_ok() {
        let mut buf: [u8; 1024] = [0; 1024];
        let read_res: Result<usize, Error> = stream.read(&mut buf);
        let resp: parser::RespType;

        match read_res {
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
        let parsed_response: RespType  = srv.execute_resp(resp);
        let serialized_response: String = parsed_response.to_resp_string();
        let _ = stream.write(serialized_response.as_bytes());
    }
}

fn main() {
    let mut port = DEFAULT_PORT;
    let args: Vec<String> = env::args().collect();
    if args.len() > 1 {
        let flag = &args[1];
        match flag.as_str() {
            "--port" => {
                if args.len() > 2 {
                    port = args[2].parse::<u16>().unwrap();
                } else {
                    eprintln!("Port number not provided");
                    return;
                }
            }
            _ => {
                eprintln!("Unknown flag: {}", flag);
                return;
            }
        }
    }

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).unwrap();

    for stream in listener.incoming() {
        println!("Found stream, handling connection:");
        match stream {
            Ok(mut _stream) => {
                println!("accepted new connection");
                thread::spawn(move || {
                    handle_client(_stream);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
