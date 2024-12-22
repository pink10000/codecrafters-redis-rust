use crate::{parser::{parse_resp, RespType}, role};
use std::{
    collections::HashMap,
    fmt::format,
    io::{Read, Write},
    net::TcpStream,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use role::Role;

#[derive(Clone)]
pub struct ServerAddr {
    pub _ip: String,
    pub _port: u16,
}

impl ServerAddr {
    pub fn new(_ip: String, _port: u16) -> Self {
        Self { _ip, _port }
    }
}

const EMPTY_RDB_FILE: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

#[derive(Clone)]
pub struct ServerState {
    db: HashMap<String, String>,
    expiry: HashMap<String, Instant>,
    replication_id: Option<String>,
    replication_offset: Option<u64>,
    _port: u16,
    replica_of: Option<ServerAddr>,

    slave_servers: Vec<Arc<Mutex<TcpStream>>>,
}

/*
Data structure for the server state.
- db: HashMap<String, String> to store key-value pairs.
- expiry: HashMap<String, Instant> to store expiry time for keys.
- replication_id: Option<String> to store the replication id. This
  value is Some if the server is a master. Otherwise, it is None.
- replication_offset: Option<String> to store the replication. Thus
  value is Some if the server is a master. Otherwise, it is None.
*/
impl ServerState {
    pub fn new(port: u16, replica_of: Option<ServerAddr>) -> Self {
        let mut repl_id: Option<String> = None;
        let mut repl_offset: Option<u64> = None;
        match replica_of {
            Some(_) => {}
            None => {
                repl_id = Some("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string());
                repl_offset = Some(0);
            }
        }
        ServerState {
            db: HashMap::new(),
            expiry: HashMap::new(),
            _port: port,
            replication_id: repl_id,
            replication_offset: repl_offset,
            replica_of: replica_of,
            slave_servers: Vec::new(),
        }
    }

    pub fn get_role(&self) -> Role {
        match self.replica_of {
            Some(_) => Role::Slave,
            None => Role::Master,
        }
    }

    pub fn get_replica_of(&self) -> Option<ServerAddr> {
        self.replica_of.clone()
    }

    pub fn retain_slave(&mut self, stream: TcpStream) {
        println!(
            "Retaining active slave stream: {}",
            stream.peer_addr().unwrap()
        );
        self.slave_servers.push(Arc::new(Mutex::new(stream))); // Store the stream
    }

    /*
    So far, most execuations require the type to be a  RespType::Array.
    */
    pub fn execute_resp(&mut self, resp: RespType) -> RespType {
        match resp {
            RespType::Array(arr) => self.execute_array(arr),
            RespType::BulkString(str) => RespType::BulkString(str),
            RespType::SimpleString(_) => todo!(),
            RespType::Error(_) => todo!(),
            RespType::Integer(_) => todo!(),
            RespType::NullBulkString => RespType::NullBulkString,
            RespType::NullArray => RespType::NullArray,
        }
    }

    /*
    Command is always the first element in the array.
     */
    fn execute_array(&mut self, arr: Vec<RespType>) -> RespType {
        match arr[0].clone() {
            RespType::BulkString(str) => match str.to_lowercase().as_str() {
                "ping" => RespType::SimpleString("PONG".to_string()),
                "echo" => self.execute_resp(arr[1].clone()),
                "set" => self.handle_set(arr),
                "get" => self.handle_get(arr),
                "info" => self.handle_info(arr),
                "replconf" => self.handle_replconf(arr),
                "psync" => self.handle_psync(arr),
                "command" => RespType::Error("Not implemented".to_string()),
                _ => RespType::Error("ERR unknown command".to_string()),
            },
            _ => RespType::Error("ERR unknown command".to_string()),
        }
    }

    fn handle_set(&mut self, arr: Vec<RespType>) -> RespType {
        let key: String = match self.execute_resp(arr[1].clone()) {
            RespType::BulkString(s) => s,
            _ => return RespType::Error("ERR key is not a valid BulkString".to_string()),
        };
        let value: String = match self.execute_resp(arr[2].clone()) {
            RespType::BulkString(s) => s,
            _ => return RespType::Error("ERR value is not a valid BulkString".to_string()),
        };

        println!("-inserted ({}, {})", key, value);
        if arr.len() == 3 {
            self.db.insert(key.clone(), value.clone());
            self.propagate_set(key.clone(), value.clone(), None);
            return RespType::SimpleString("OK".to_string());
        }
        match arr[3].clone() {
            RespType::BulkString(str) => match str.to_lowercase().as_str() {
                "px" => match arr[4].clone() {
                    RespType::BulkString(str) => {
                        let expiry: u64 = str.parse().unwrap();
                        let expiry_time = Instant::now()
                            .checked_add(Duration::from_millis(expiry))
                            .unwrap();
                        self.db.insert(key.clone(), value.clone());
                        self.expiry.insert(key.clone(), expiry_time);
                        self.propagate_set(key.clone(), value.clone(), Some(expiry));
                        RespType::SimpleString("OK".to_string())
                    }
                    _ => RespType::Error("ERR value is not a valid RESP type".to_string()),
                },
                _ => RespType::Error("ERR value is not a valid RESP type".to_string()),
            },
            _ => RespType::Error("ERR value is not a valid RESP type".to_string()),
        }
    }

    fn handle_get(&mut self, arr: Vec<RespType>) -> RespType {
        let key: String = match self.execute_resp(arr[1].clone()) {
            RespType::BulkString(s) => s,
            _ => return RespType::Error("ERR key is not a valid BulkString".to_string()),
        };
        self.check_expiry();
        match self.db.get(&key) {
            Some(val) => {
                println!("Value: {}", val);
                RespType::SimpleString(val.clone())
            }
            None => RespType::NullBulkString,
        }
    }

    fn handle_info(&self, arr: Vec<RespType>) -> RespType {
        match arr[1].clone() {
            RespType::BulkString(str) => match str.to_lowercase().as_str() {
                "replication" => {
                    let mut output: Vec<String> = Vec::new();
                    let role = self.get_role();
                    output.push(format!("role:{}", role));
                    if role == Role::Master {
                        output.push(format!(
                            "master_replid:{}",
                            self.replication_id.clone().unwrap()
                        ));
                        output.push(format!(
                            "master_repl_offset:{}",
                            self.replication_offset.clone().unwrap()
                        ));
                    }
                    RespType::BulkString(output.join("\n"))
                }
                _ => RespType::Error("ERR unknown subcommand".to_string()),
            },
            _ => RespType::Error("ERR unknown subcommand".to_string()),
        }
    }

    fn handle_replconf(&mut self, arr: Vec<RespType>) -> RespType {
        // need to extract the information from the arr
        match arr[1].clone() {
            RespType::BulkString(str) => match str.to_lowercase().as_str() {
                "listening-port" => {
                    let port: u16 = match arr[2].clone() {
                        RespType::BulkString(str) => str.parse().unwrap(),
                        _ => {
                            return RespType::Error(
                                "ERR port is not a valid BulkString".to_string(),
                            )
                        }
                    };
                    println!("-Received slave port: {}", port);
                    RespType::SimpleString("OK".to_string())
                },
                "capa" => {
                    let capa: String = match arr[2].clone() {
                        RespType::BulkString(str) => str,
                        _ => {
                            return RespType::Error(
                                "ERR capa is not a valid BulkString".to_string(),
                            )
                        }
                    };
                    if capa == "psync2" {
                        RespType::SimpleString("OK".to_string())
                    } else {
                        RespType::Error("ERR unknown capability".to_string())
                    }
                },
                "getack" => {
                    let offset: u64 = match arr[2].clone() {
                        RespType::BulkString(str) => str.parse().unwrap(),
                        _ => {
                            return RespType::Error(
                                "ERR offset is not a valid BulkString".to_string(),
                            )
                        }
                    };
                    self.replication_offset = Some(offset);
                    RespType::Array(vec![
                        RespType::BulkString("REPLCONF".to_string()),
                        RespType::BulkString("ACK".to_string()),
                        RespType::BulkString("0".to_string()),
                    ])
                }
                _ => RespType::Error("ERR unknown subcommand".to_string()),
            },
            _ => RespType::Error("ERR unknown subcommand".to_string()),
        }
    }

    /*
    Sync the data from the master to the slave
     */
    fn handle_psync(&mut self, _arr: Vec<RespType>) -> RespType {
        // will always send this, change on first sync call on ?
        let out: String = format!(
            "FULLRESYNC {} {}",
            self.replication_id.clone().unwrap(),
            self.replication_offset.clone().unwrap()
        );
        RespType::SimpleString(out)
    }

    /*
    Hard coded empty file.
    */
    pub fn full_resync(&mut self) -> (String, Vec<u8>) {
        let rdb_bytes: Vec<u8> = hex::decode(EMPTY_RDB_FILE).unwrap();
        (format!("${}\r\n", rdb_bytes.len()), rdb_bytes)
    }

    pub fn propagate_set(&mut self, key: String, value: String, expiry: Option<u64>) {
        let mut cmd: Vec<RespType> = vec![
            RespType::BulkString("SET".to_string()),
            RespType::BulkString(key.clone()),
            RespType::BulkString(value.clone()),
        ];
        match expiry {
            Some(time) => {
                cmd.push(RespType::BulkString("PX".to_string()));
                cmd.push(RespType::BulkString(time.to_string()));
            }
            None => {}
        }
        let serialized_command: String = RespType::Array(cmd).to_resp_string();
        for slave in &self.slave_servers {
            let mut stream = slave.lock().unwrap();
            if let Err(e) = stream.write_all(serialized_command.as_bytes()) {
                eprintln!(
                    "Failed to send command to slave {}: {:?}",
                    stream.peer_addr().unwrap(),
                    e
                );
            } else {
                println!(
                    "Successfully propagated command to slave {}",
                    stream.peer_addr().unwrap()
                );
            }
        }
    }

    fn check_expiry(&mut self) {
        let now = Instant::now();
        let mut expired_keys: Vec<String> = Vec::new();
        for (key, expiry) in self.expiry.iter() {
            if now > *expiry {
                expired_keys.push(key.clone());
            }
        }
        for key in expired_keys {
            self.db.remove(&key);
            self.expiry.remove(&key);
        }
    }
}
