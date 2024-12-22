use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum Role {
    Master,
    Slave,
}
impl Role {
    pub fn as_str(&self) -> &str {
        match self {
            Role::Master => "master",
            Role::Slave => "slave",
        }
    }
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Role::Master => write!(f, "master"),
            Role::Slave => write!(f, "slave"),
        }
    }
}