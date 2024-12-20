use crate::parser::RespType;
use std::{collections::HashMap, time::Duration, time::Instant};

pub struct ServerAddr {
    _ip: String,
    _port: u16,
}

impl ServerAddr {
    pub fn new(_ip: String, _port: u16) -> Self {
        Self { _ip, _port }
    }
}

pub struct ServerState {
    db: HashMap<String, String>,
    expiry: HashMap<String, Instant>,
    _port: u16,
    _replica_of: Option<ServerAddr>,
}

/*
Data structure for the server state.
- db: HashMap<String, String> to store key-value pairs.
- expiry: HashMap<String, Instant> to store expiry time for keys.
*/
impl ServerState {
    pub fn new(port: u16, replica_of: Option<ServerAddr>) -> Self {
        ServerState {
            db: HashMap::new(),
            expiry: HashMap::new(),
            _port: port,
            _replica_of: replica_of,
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

    fn execute_array(&mut self, arr: Vec<RespType>) -> RespType {
        let cmd = arr[0].clone();
        match cmd {
            RespType::BulkString(str) => match str.to_lowercase().as_str() {
                "ping" => RespType::SimpleString("PONG".to_string()),
                "echo" => self.execute_resp(arr[1].clone()),
                "set" => self.handle_set(arr),
                "get" => self.handle_get(arr),
                "info" => self.handle_info(arr),
                _ => RespType::Error("ERR unknown command".to_string()),
            },
            _ => RespType::Error("ERR unknown command".to_string()),
        }
    }

    fn handle_set(&mut self, arr: Vec<RespType>) -> RespType {
        let key = self.execute_resp(arr[1].clone()).to_resp_string();
        let value = self.execute_resp(arr[2].clone()).to_resp_string();
        if arr.len() == 3 {
            self.db.insert(key.clone(), value);
            return RespType::SimpleString("OK".to_string());
        }

        match arr[3].clone() {
            RespType::BulkString(str) => match str.to_lowercase().as_str() {
                "px" => {
                    let expiry: String = arr[4].clone().to_resp_string();
                    let expiry: u64 = expiry.parse().unwrap();
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
        }
    }

    fn handle_get(&mut self, arr: Vec<RespType>) -> RespType {
        let key = self.execute_resp(arr[1].clone()).to_resp_string();
        self.check_expiry();
        match self.db.get(&key) {
            Some(val) => RespType::BulkString(val.clone()),
            None => RespType::NullBulkString,
        }
    }

    fn handle_info(&self, arr: Vec<RespType>) -> RespType {
        match arr[1].clone() {
            RespType::BulkString(str) => match str.to_lowercase().as_str() {
                "replication" => RespType::BulkString("role:master".to_string()),
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
