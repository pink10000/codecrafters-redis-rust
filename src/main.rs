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
        let read_res: Result<usize, Error> = stream.read(&mut buf);
        let resp: parser::RespType;

        match read_res {
            Ok(0) => return,
            Err(_) => return,
            Ok(size) => {
                let command = String::from_utf8_lossy(&buf[..size]);
                resp = parser::parse_resp(&command).unwrap();
                println!("{} received command: {:?}", role, resp);
            }
        }

        // before it parses the response and and changes the state of the server
        // it needs to lock the server state, so that no other thread can access it
        // this scope is NECESSARY to ENSURE the lock is released.
        {
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
}

fn request_replication(
    server_state: Arc<Mutex<ServerState>>,
    self_port: u16,
    master_ip: String,
    master_port: u16,
) {
    // send ping
    let mut stream = TcpStream::connect(format!("{}:{}", master_ip, master_port)).unwrap();
    let serial_ping: String =
        RespType::Array(vec![RespType::BulkString("PING".to_string())]).to_resp_string();
    let _ = stream.write(serial_ping.as_bytes());

    // read pong
    let mut buf: [u8; 1024] = [0; 1024];
    let _ = stream.read(&mut buf);
    let pong = String::from_utf8_lossy(&buf);
    print!(" Received pong: {}", pong);

    // send replication request
    let serial_listening_port: String = RespType::Array(vec![
        RespType::BulkString("REPLCONF".to_string()),
        RespType::BulkString("listening-port".to_string()),
        RespType::BulkString(format!("{}", self_port)),
    ])
    .to_resp_string();
    let _ = stream.write(serial_listening_port.as_bytes());

    // read replication response
    let mut buf: [u8; 1024] = [0; 1024];
    let _ = stream.read(&mut buf);
    let replconf = String::from_utf8_lossy(&buf);
    print!(" Received replconf: {}", replconf);

    // send capabilitiy sync
    let serial_capa_sync: String = RespType::Array(vec![
        RespType::BulkString("REPLCONF".to_string()),
        RespType::BulkString("capa".to_string()),
        RespType::BulkString("psync2".to_string()),
    ])
    .to_resp_string();
    let _ = stream.write(serial_capa_sync.as_bytes());

    // read replication response
    let mut buf: [u8; 1024] = [0; 1024];
    let _ = stream.read(&mut buf);
    let replconf = String::from_utf8_lossy(&buf);
    print!(" Received replconf: {}", replconf);

    // send psync
    let serial_psync: String = RespType::Array(vec![
        RespType::BulkString("PSYNC".to_string()),
        RespType::BulkString("?".to_string()),
        RespType::BulkString("-1".to_string()),
    ])
    .to_resp_string();
    let _ = stream.write(serial_psync.as_bytes());

    // read psync response (replication id and offset)
    let mut buf: [u8; 1024] = [0; 1024];
    let _ = stream.read(&mut buf);
    let _replconf = String::from_utf8_lossy(&buf);

    // read psync response (rdb file)
    let mut buf: [u8; 1024] = [0; 1024];
    let _ = stream.read(&mut buf);
    let _rdb = &buf;
    println!(" Received rdb: (OUTPUT OMITTED)\n");

    continuous_replication(server_state, stream);
}

fn continuous_replication(server_state: Arc<Mutex<ServerState>>, mut stream: TcpStream) {
    // server needs to stay alive to handle replications
    // minimizes lock contention
    loop {
        let mut buf = [0u8; 1024];
        let msg: RespType = match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(size) => {
                // parse the incoming RESP command
                let command = String::from_utf8_lossy(&buf[..size]);
                match parse_resp(&command) {
                    Ok(r) => {
                        println!(" slave received command: {:?}", r);
                        r
                    }
                    Err(e) => {
                        eprintln!(" error parsing replication RESP: {:?}", e);
                        return;
                    }
                }
            }
            Err(_) => return,
        };
        let resp: RespType = server_state.lock().unwrap().execute_resp(msg.clone());
        println!(" slave sent response: {:?}", resp);
        if resp.to_resp_string().contains("ACK") {
            let _ = stream.write(resp.to_resp_string().as_bytes());
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

    let srv = ServerState::new(port, replica_of);
    let server_state = Arc::new(Mutex::new(srv));
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).unwrap();

    // needs to request replication if the server is a slave
    let srv_role: Role = server_state.lock().unwrap().get_role();
    println!("Server role: {:?}\n", srv_role);

    // if the server is a slave, then request continuous replication
    if srv_role == Role::Slave {
        println!("----- Requesting replication...");
        let server_state_clone = Arc::clone(&server_state);
        let replica_of = server_state
            .lock()
            .unwrap()
            .get_replica_of()
            .clone()
            .unwrap();
        std::thread::spawn(move || {
            request_replication(server_state_clone, port, replica_of._ip, replica_of._port);
            println!("-No longer replicating from master.");
        });
    }

    // server state that is shared between threads
    for stream in listener.incoming() {
        println!("\nFound stream, handling connection:");
        match stream {
            Ok(stream) => {
                let srv_clone = Arc::clone(&server_state);
                let srv_role_clone = srv_role.clone();
                thread::spawn(move || handle_client(stream, &srv_clone, srv_role_clone));
            }
            Err(e) => eprintln!("error: {}", e),
        }
    }
}
