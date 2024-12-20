#![allow(unused_imports)]
pub mod parser;
pub mod server;

use std::{
    io::{Read, Write, Error},
    net::{TcpListener, TcpStream},
    thread,
};

use server::ServerState;

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
        let serialized_response: String = srv.execute_resp(resp);
        let _ = stream.write(serialized_response.as_bytes());
    }
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

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
