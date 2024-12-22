#![allow(unused_imports)]
pub mod parser;
pub mod server;
pub mod role;

use std::{
    env,
    io::{Error, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
};

use parser::{parse_resp, parse_retain_cmd, RespType};
use server::{ServerAddr, ServerState};
use role::Role;

const DEFAULT_PORT: u16 = 6379;

fn handle_client(mut stream: TcpStream, srv: &Arc<Mutex<ServerState>>, role: Role) {
    loop {
        let mut buf: [u8; 1024] = [0; 1024];
        // Read from the client, no lock held
        let read_res: Result<usize, Error> = stream.read(&mut buf);
        let msg: parser::RespType;

        match read_res {
            Ok(0) => return,
            Err(_) => return,
            Ok(size) => {
                let command = String::from_utf8_lossy(&buf[..size]);
                match parse_resp(&command) {
                    Ok(parsed) => {
                        println!("{} received command: {:?}", role, parsed);
                        msg = parsed;
                    }
                    Err(e) => {
                        eprintln!("Failed to parse client command: {}", e);
                        return;
                    }
                }
            }
        }

        // EDIT: Combine lock usage so we only lock/unlock once
        {
            let mut guard = srv.lock().unwrap();

            // Update replication offset
            guard.update_replication_offset(msg.clone());

            // Execute the command
            let parsed_response: RespType = guard.execute_resp(msg.clone());
            let serialized_response = parsed_response.to_resp_string();

            // Send response
            if let Err(e) = stream.write(serialized_response.as_bytes()) {
                eprintln!("Failed writing response to client: {}", e);
                return;
            }
            println!("-Sent response: {:?}", serialized_response);

            // If it's REPLCONF listening-port, store the connection
            if parse_retain_cmd(&msg) {
                match stream.try_clone() {
                    Ok(cloned_stream) => {
                        guard.retain_slave(cloned_stream);
                    }
                    Err(e) => {
                        eprintln!("Failed to clone stream: {}", e);
                    }
                }
            }

            // If we returned a FULLRESYNC line, also send the RDB
            if serialized_response.contains("FULLRESYNC") {
                let (header, rdb) = guard.full_resync();
                let _ = stream.write(header.as_bytes());
                let _ = stream.write(&rdb);
            }
        } // lock is released here
    }
}

// EDIT: Removed reading the RDB bytes inside request_replication().
//       Instead, we directly call continuous_replication() after the handshake.
fn request_replication(
    server_state: Arc<Mutex<ServerState>>,
    self_port: u16,
    master_ip: String,
    master_port: u16,
) {
    let mut stream = TcpStream::connect(format!("{}:{}", master_ip, master_port)).unwrap();

    // Send PING
    let serial_ping = RespType::Array(vec![RespType::BulkString("PING".into())])
        .to_resp_string();
    let _ = stream.write(serial_ping.as_bytes());

    // Read PONG
    let mut buf = [0u8; 1024];
    let _ = stream.read(&mut buf);
    let pong = String::from_utf8_lossy(&buf);
    println!(" Received pong: {}", pong);

    // REPLCONF listening-port
    let serial_listening_port = RespType::Array(vec![
        RespType::BulkString("REPLCONF".into()),
        RespType::BulkString("listening-port".into()),
        RespType::BulkString(format!("{}", self_port)),
    ])
    .to_resp_string();
    let _ = stream.write(serial_listening_port.as_bytes());

    // Read replication response
    let mut buf = [0u8; 1024];
    let _ = stream.read(&mut buf);
    let replconf = String::from_utf8_lossy(&buf);
    print!(" Received replconf: {}", replconf);

    // REPLCONF capa psync2
    let serial_capa_sync = RespType::Array(vec![
        RespType::BulkString("REPLCONF".into()),
        RespType::BulkString("capa".into()),
        RespType::BulkString("psync2".into()),
    ])
    .to_resp_string();
    let _ = stream.write(serial_capa_sync.as_bytes());

    // Read replication response
    let mut buf = [0u8; 1024];
    let _ = stream.read(&mut buf);
    let replconf = String::from_utf8_lossy(&buf);
    print!(" Received replconf: {}", replconf);

    // PSYNC ? -1
    let serial_psync = RespType::Array(vec![
        RespType::BulkString("PSYNC".into()),
        RespType::BulkString("?".into()),
        RespType::BulkString("-1".into()),
    ])
    .to_resp_string();
    let _ = stream.write(serial_psync.as_bytes());

    // Read psync response (FULLRESYNC + offset line)
    let mut buf = [0u8; 1024];
    let _ = stream.read(&mut buf);
    // We won't parse it strictly here, just ignore or log
    let _psync_reply = String::from_utf8_lossy(&buf);

    // EDIT: DO NOT read the RDB here in a single call. Instead, let continuous_replication() read everything.
    println!(" Received rdb: (OUTPUT OMITTED)\n");

    // Now do the loop that continuously reads
    continuous_replication(server_state, stream);
}

fn continuous_replication(server_state: Arc<Mutex<ServerState>>, mut stream: TcpStream) {
    loop {
        let mut buf = [0u8; 1024];
        let n = match stream.read(&mut buf) {
            Ok(0) => {
                println!(" Master closed the connection. Stopping replication loop.");
                return;
            }
            Ok(size) => size,
            Err(e) => {
                eprintln!("Error reading from master: {}", e);
                return;
            }
        };

        // Parse the incoming data
        let command_str = String::from_utf8_lossy(&buf[..n]);
        match parse_resp(&command_str) {
            Ok(msg) => {
                println!(" slave received command: {:?}", msg);

                // EDIT: Single lock scope
                {
                    let mut guard = server_state.lock().unwrap();
                    guard.update_replication_offset(msg.clone());
                    let resp = guard.execute_resp(msg.clone());

                    // Possibly respond if we see "ACK"
                    let serialized_resp = resp.to_resp_string();
                    println!(" slave sent response: {:?}", serialized_resp);
                    if serialized_resp.contains("ACK") {
                        // If you want to only respond if it's REPLCONF GETACK, do that check
                        if let Err(e) = stream.write_all(serialized_resp.as_bytes()) {
                            eprintln!("Error sending ack to master: {}", e);
                            return;
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!(" error parsing replication RESP: {:?}", e);
                return;
            }
        }
    }
}

fn main() {
    let mut port: u16 = DEFAULT_PORT;
    let mut replica_of: Option<ServerAddr> = None;

    let args: Vec<String> = env::args().collect();
    let mut idx: usize = 1; // skip the binary name

    while idx < args.len() {
        let arg = &args[idx];
        match arg.as_str() {
            "--port" => {
                if args.len() > idx + 1 {
                    port = args[idx + 1].parse::<u16>().unwrap();
                    idx += 1;
                } else {
                    eprintln!("Port number not provided");
                    return;
                }
            }
            "--replicaof" => {
                if args.len() > idx + 1 {
                    let ip_port = args[idx + 1].clone();
                    let mut split = ip_port.split(' ');
                    let ip = split.next().unwrap().to_string();
                    let prt = split.next().unwrap().parse::<u16>().unwrap();
                    replica_of = Some(ServerAddr::new(ip, prt));
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

    // Build server state
    let state = ServerState::new(port, replica_of);
    let server_state = Arc::new(Mutex::new(state));
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).unwrap();

    // If we are a slave, spawn the replication thread
    let srv_role = server_state.lock().unwrap().get_role();
    println!("Server role: {:?}\n", srv_role);

    if srv_role == Role::Slave {
        println!("----- Requesting replication...");
        let clone_for_repl = Arc::clone(&server_state);
        let rep_of = clone_for_repl
            .lock()
            .unwrap()
            .get_replica_of()
            .expect("we are a slave, so must have a replica_of");
        std::thread::spawn(move || {
            request_replication(clone_for_repl, port, rep_of._ip, rep_of._port);
            println!("-No longer replicating from master.");
        });
    }

    // Accept local client connections
    for incoming in listener.incoming() {
        println!("\nFound stream, handling connection:");
        match incoming {
            Ok(stream) => {
                let srv_clone = Arc::clone(&server_state);
                let r_clone = srv_role.clone();
                thread::spawn(move || handle_client(stream, &srv_clone, r_clone));
            }
            Err(e) => eprintln!("error: {}", e),
        }
    }
}
