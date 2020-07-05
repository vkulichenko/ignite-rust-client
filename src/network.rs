use std::net::TcpStream;
use std::io::{Write, Read};

use bytes::{BytesMut, Bytes, Buf, BufMut};

use crate::error::{Result, ErrorKind, Error};
use crate::{VERSION, Version};
use crate::binary::{Value, BinaryWrite};
use crate::configuration::Configuration;

pub(crate) struct Tcp {
    pub(crate) stream: TcpStream,
}

impl Tcp {
    pub(crate) fn handshake(&mut self, config: &Configuration) -> Result<()> {
        let mut request = BytesMut::with_capacity(8);

        request.put_i8(1);
        request.put_i16_le(VERSION.major);
        request.put_i16_le(VERSION.minor);
        request.put_i16_le(VERSION.patch);
        request.put_i8(2);

        if let Some(username) = config.username.clone() {
            username.write(&mut request)?;

            config.password.clone().write(&mut request);
        }

        let mut response = self.send(&request)?;

        let success = response.get_u8();

        if success == 1 {
            Ok(())
        }
        else {
            let major = response.get_i16_le();
            let minor = response.get_i16_le();
            let patch = response.get_i16_le();

            let kind = ErrorKind::Handshake {server_version: Version { major, minor, patch }, client_version: VERSION };

            let message = Value::read(&mut response)?;

            let message = match message {
                Some(Value::String(message)) => message,
                _ => "Handshake unexpected failure".to_string(),
            };

            Err(Error::new(kind, message))
        }
    }

    pub(crate) fn execute<R, F1, F2>(&mut self, operation_code: i16, request_writer: F1, response_reader: F2) -> Result<R>
        where
            F1: Fn(&mut BytesMut) -> Result<()>,
            F2: Fn(&mut Bytes) -> Result<R>,
    {
        let mut request = BytesMut::with_capacity(1024);

        request.put_i16_le(operation_code);
        request.put_i64_le(0); // Request ID.

        request_writer(&mut request)?;

        let mut response = self.send(&request)?;

        assert_eq!(response.get_i64_le(), 0); // Request ID.

        let status = response.get_i32_le();

        if status == 0 {
            response_reader(&mut response)
        }
        else {
            let message = String::from_utf8(response.to_vec())?;

            Err(Error::new(ErrorKind::Ignite(status), message))
        }
    }

    fn send(&mut self, msg: &BytesMut) -> Result<Bytes> {
        // Write.

        let len = msg.len() as i32;
        let len = len.to_le_bytes();

        self.stream.write_all(&len)?;
        self.stream.write_all(msg.as_ref())?;
        self.stream.flush()?;

        // Read.

        let mut len = [0u8; 4];

        self.stream.read_exact(&mut len)?;

        let len = Bytes::from(len.to_vec()).get_i32_le();

        let mut msg = vec![0u8; len as usize];

        self.stream.read_exact(&mut msg)?;

        Ok(Bytes::from(msg))
    }
}
