#![allow(unused_imports)]
use std::{io::Write, net::{TcpListener, TcpStream}};

fn handle_client(mut stream: TcpStream) {

    let pong_str: String = "+PONG\r\n".to_string();
    let _ = stream.write(pong_str.as_bytes());
    
}



fn main() {    
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                println!("accepted new connection");
                handle_client(_stream);



            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
