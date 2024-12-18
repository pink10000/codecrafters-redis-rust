#![allow(unused_imports)]
use std::{
    io::Write,
    net::{TcpListener, TcpStream},
};

fn handle_client(stream: &mut TcpStream) {
    let pong_str: String = "+PONG\r\n".to_string();
    let _ = stream.write(pong_str.as_bytes());
}

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                println!("accepted new connection");
                // while _stream.peek(&mut [0; 1]).is_ok() {
                handle_client(&mut _stream);
                // }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
