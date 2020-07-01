use std::net::TcpStream;
use std::io::{Write, Read};
use std::cell::RefMut;

use bytes::{BytesMut, Bytes, Buf};

use crate::error::Result;

pub(crate) fn send(stream: &mut RefMut<TcpStream>, msg: &BytesMut) -> Result<Bytes> {
    // Write.

    let len = msg.len() as i32;
    let len = len.to_le_bytes();

    stream.write_all(&len)?;
    stream.write_all(msg.as_ref())?;
    stream.flush()?;

    // Read.

    let mut len = [0u8; 4];

    stream.read_exact(&mut len)?;

    let len = Bytes::from(len.to_vec()).get_i32_le();

    let mut msg = vec![0u8; len as usize];

    stream.read_exact(&mut msg)?;

    Ok(Bytes::from(msg))
}
