use bytes::{Bytes, BytesMut};
use std::iter::{self, Peekable};

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

pub fn execute_resp(resp: RespType) -> String {
    match resp {
        RespType::Array(arr) => {
            let cmd = arr[0].clone();
            match cmd {
                RespType::BulkString(str) => match str.to_lowercase().as_str() {
                    "ping" => "+PONG\r\n".to_string(),
                    "echo" => format!("{}\r\n", execute_resp(arr[1].clone())),
                    _ => "-ERR unknown command\r\n".to_string(),
                },
                _ => "-ERR unknown command\r\n".to_string(),
            }
        }
        RespType::BulkString(str) => {
            str
        }
        RespType::SimpleString(_) => todo!(),
        RespType::Error(_) => todo!(),
        RespType::Integer(_) => todo!(),
        RespType::NullBulkString => todo!(),
        RespType::NullArray => todo!(),
    }
}

pub fn parse_resp(input: &str) -> Result<RespType, String> {
    println!("{:?}", input.chars());
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
            Err("Invalid RESP data type".to_string())
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
