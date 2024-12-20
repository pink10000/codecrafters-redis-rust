use crate::parser::RespType;
use std::{collections::HashMap, time::Duration, time::Instant};

pub struct ServerState {
    db: HashMap<String, String>,
    expiry: HashMap<String, Instant>,
}

impl ServerState {
    pub fn new() -> Self {
        ServerState {
            db: HashMap::new(),
            expiry: HashMap::new(),
        }
    }

    pub fn execute_resp(&mut self, resp: RespType) -> String {
        match resp {
            RespType::Array(arr) => self.execute_array(arr),
            RespType::BulkString(str) => str,
            RespType::SimpleString(_) => todo!(),
            RespType::Error(_) => todo!(),
            RespType::Integer(_) => todo!(),
            RespType::NullBulkString => format!("$-1\r\n"),
            RespType::NullArray => todo!(),
        }
    }

    fn execute_array(&mut self, arr: Vec<RespType>) -> String {
        if arr.is_empty() {
            return "-ERR empty array\r\n".to_string();
        }

        let cmd = arr[0].clone();
        match cmd {
            RespType::BulkString(str) => match str.to_lowercase().as_str() {
                "ping" => "+PONG\r\n".to_string(),
                "echo" => {
                    let str: String = self.execute_resp(arr[1].clone());
                    format!("${}\r\n{}\r\n", str.len(), str)
                }
                "set" => self.handle_set(arr),
                "get" => self.handle_get(arr),
                "info" => self.handle_info(arr),
                _ => "-ERR unknown command\r\n".to_string(),
            },
            _ => "-ERR unknown command\r\n".to_string(),
        }
    }

    fn handle_set(&mut self, arr: Vec<RespType>) -> String {
        let key = self.execute_resp(arr[1].clone());
        let value = self.execute_resp(arr[2].clone());
        if arr.len() == 3 {
            self.db.insert(key.clone(), value);
            return "+OK\r\n".to_string();
        }

        match arr[3].clone() {
            RespType::BulkString(str) => match str.to_lowercase().as_str() {
                "px" => {
                    let expiry: String = self.execute_resp(arr[4].clone());
                    let expiry: u64 = expiry.parse().unwrap();
                    let expiry_time = Instant::now()
                        .checked_add(Duration::from_millis(expiry))
                        .unwrap();

                    self.db.insert(key.clone(), value);
                    self.expiry.insert(key.clone(), expiry_time);
                    "+OK\r\n".to_string()
                }
                _ => "-ERR value is not a valid RESP type\r\n".to_string(),
            },
            _ => "-ERR value is not a valid RESP type\r\n".to_string(),
        }
    }

    fn handle_get(&mut self, arr: Vec<RespType>) -> String {
        let key = self.execute_resp(arr[1].clone());
        self.check_expiry();
        match self.db.get(&key) {
            Some(val) => format!("${}\r\n{}\r\n", val.len(), val),
            None => self.execute_resp(RespType::NullBulkString),
        }
    }

    fn handle_info(&self, arr: Vec<RespType>) -> String {
        match arr[1].clone() {
            RespType::BulkString(str) => match str.to_lowercase().as_str() {
                "replication" => {
                    format!("$11\r\nrole:master\r\n")
                }
                _ => "-ERR unknown subcommand\r\n".to_string(),
            },
            _ => "-ERR unknown subcommand\r\n".to_string(),
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
