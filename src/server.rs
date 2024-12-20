use std::collections::HashMap;
use crate::parser::RespType;

pub struct ServerState {
    db: HashMap<String, String>,
}

impl ServerState {
    pub fn new() -> Self {
        ServerState { db: HashMap::new() }
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
                },
                "set" => {
                    let key = self.execute_resp(arr[1].clone());
                    let value = self.execute_resp(arr[2].clone());
                    self.db.insert(key, value);
                    format!("+OK\r\n")
                },
                "get" => {
                    let key = self.execute_resp(arr[1].clone());
                    match self.db.get(&key) {
                        Some(value) => format!("${}\r\n{}\r\n", value.len(), value),
                        None => self.execute_resp(RespType::NullBulkString),
                    }
                }
                _ => "-ERR unknown command\r\n".to_string(),
            },
            _ => "-ERR unknown command\r\n".to_string(),
        }
    }
}