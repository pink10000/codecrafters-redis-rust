use crate::parser::RespType;
use std::{collections::HashMap, time::Duration, time::Instant};

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

#[derive(Clone)]
pub struct ServerState {
    db: HashMap<String, String>,
    expiry: HashMap<String, Instant>,
    replication_id: Option<String>,
    replication_offset: Option<u64>,
    _port: u16,
    pub replica_of: Option<ServerAddr>,
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
            },
        }
        ServerState {
            db: HashMap::new(),
            expiry: HashMap::new(),
            _port: port,
            replication_id: repl_id,
            replication_offset: repl_offset,
            replica_of: replica_of,
        }
    }

    pub fn get_role(&self) -> String {
        match self.replica_of {
            Some(_) => "slave".to_string(),
            None => "master".to_string(),
        }
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
            self.db.insert(key.clone(), value);
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
                        self.db.insert(key.clone(), value);
                        self.expiry.insert(key.clone(), expiry_time);
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
                    if role == "master" {
                        output.push(format!("master_replid:{}", self.replication_id.clone().unwrap()));
                        output.push(format!("master_repl_offset:{}", self.replication_offset.clone().unwrap()));
                    }
                    RespType::BulkString(output.join("\n"))
                }
                _ => RespType::Error("ERR unknown subcommand".to_string()),
            },
            _ => RespType::Error("ERR unknown subcommand".to_string()),
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
