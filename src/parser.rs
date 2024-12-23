use bytes::{Bytes, BytesMut};
use std::{
    collections::HashMap,
    iter::{self, Peekable},
};

#[derive(Debug, PartialEq, Clone)]
pub enum RespType {
    Array(Vec<RespType>),
    BulkString(String),
    SimpleString(String),
    Error(String),
    Integer(i64),
    NullBulkString,
    NullArray,
}

impl RespType {
    pub fn to_resp_string(&self) -> String {
        match self {
            RespType::Array(vec) => {
                if vec.is_empty() {
                    "$-1\r\n".to_string() // Null array
                } else {
                    let elements: String = vec.iter().map(|e| e.to_resp_string()).collect();
                    format!("*{}\r\n{}", vec.len(), elements)
                }
            }
            RespType::BulkString(str) => {
                if str.is_empty() {
                    "$-1\r\n".to_string() // Null bulk string
                } else {
                    format!("${}\r\n{}\r\n", str.len(), str)
                }
            }
            RespType::SimpleString(str) => format!("+{}\r\n", str),
            RespType::Error(_) => format!("not implemented yet"),
            RespType::Integer(_) => todo!(),
            RespType::NullBulkString => "$-1\r\n".to_string(),
            RespType::NullArray => "*-1\r\n".to_string(),
        }
    }
}

pub fn parse_resp(input: &str) -> Result<RespType, String> {
    let mut chars = input.chars().peekable();
    parse_value(&mut chars)
}

pub fn parse_value<I>(chars: &mut Peekable<I>) -> Result<RespType, String>
where
    I: Iterator<Item = char>,
{
    match chars.peek() {
        Some('+') => parse_simple_string(chars),
        Some('$') => parse_bulk_string(chars),
        // Some(':') => parse_integer(chars),
        // Some('-') => parse_error(chars),
        Some('*') => parse_array(chars),
        _ => {
            let rem: String = chars.collect();
            println!("chars: {:?}", rem);
            Err("Invalid RESP type".to_string())
        }
    }
}

pub fn parse_until_crlf<I>(chars: &mut Peekable<I>) -> Result<String, String>
where
    I: Iterator<Item = char>,
{
    let mut value = String::new();
    while let Some(&c) = chars.peek() {
        if c == '\r' {
            break;
        }
        value.push(c);
        chars.next();
    }
    consume_crlf(chars)?;
    Ok(value)
}

pub fn consume_crlf<I>(chars: &mut std::iter::Peekable<I>) -> Result<(), String>
where
    I: Iterator<Item = char>,
{
    if chars.next() == Some('\r') && chars.next() == Some('\n') {
        Ok(())
    } else {
        Err("Expected CRLF".to_string())
    }
}

pub fn parse_simple_string<I>(chars: &mut Peekable<I>) -> Result<RespType, String>
where
    I: Iterator<Item = char>,
{
    chars.next(); // consume the '+' char
    let value = parse_until_crlf(chars)?;
    Ok(RespType::SimpleString(value))
}

pub fn parse_bulk_string<I>(chars: &mut Peekable<I>) -> Result<RespType, String>
where
    I: Iterator<Item = char>,
{
    chars.next(); // consume the '$' char
    let mut length_str = String::new();

    while let Some(&c) = chars.peek() {
        if c == '\r' {
            break;
        }
        length_str.push(c);
        chars.next();
    }
    consume_crlf(chars)?; // Consume the '\r\n' after the length

    let length: usize = length_str
        .parse()
        .map_err(|_| "Invalid bulk string length".to_string())?;

    // Handle null bulk string
    if length == -1_i64 as usize {
        return Ok(RespType::NullBulkString);
    }

    let mut value = String::new();

    // read bulk string content
    for _ in 0..length {
        if let Some(c) = chars.next() {
            value.push(c);
        } else {
            return Err("Unexpected end of input while reading bulk string".to_string());
        }
    }

    consume_crlf(chars)?; // Consume the '\r\n' after the bulk string content

    Ok(RespType::BulkString(value))
}

pub fn parse_array<I>(chars: &mut Peekable<I>) -> Result<RespType, String>
where
    I: Iterator<Item = char>,
{
    let mut array: Vec<RespType> = Vec::new();

    chars.next(); // consume the '*' char
    let mut length_str = String::new();
    while let Some(&c) = chars.peek() {
        // consume the length of the array
        if !c.is_digit(10) {
            break;
        }
        length_str.push(c);
        chars.next();
    }
    // need to parse the next \r\n before checking the list:
    consume_crlf(chars)?;
    // parses each element in the list
    let arr_len: usize = length_str.parse().map_err(|_| "Invalid array length")?;
    for _ in 0..arr_len {
        // will parse redis string, which needs to be parsed by the system again
        let element = parse_value(chars)?;
        array.push(element);
    }

    Ok(RespType::Array(array))
}

pub fn parse_retain_cmd(resp: &RespType) -> bool {
    if let RespType::Array(vec) = resp {
        if vec.len() == 3 {
            match (&vec[0], &vec[1], &vec[2]) {
                (
                    RespType::BulkString(cmd),
                    RespType::BulkString(arg),
                    RespType::BulkString(_port),
                ) if cmd == "REPLCONF" && arg == "listening-port" => {
                    return true;
                }
                _ => {}
            }
        }
    }
    false
}
