//! Abstractions over network streams.

use std::io;
use std::net::{TcpStream, Shutdown};
#[cfg(unix)]
use std::os::unix::net::UnixStream;

/// An abstraction over network streams.
pub trait Stream: Sized + Send + Sync + io::Read + io::Write {
    /// Creates a new independently owned handle to the underlying stream.
    fn try_clone(&self) -> io::Result<Self>;

    /// Moves this stream into or out of nonblocking mode.
    fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()>;

    /// Shuts down the read, write, or both halves of this connection.
    fn shutdown(&self, how: Shutdown) -> io::Result<()>;

    /// Splits the stream into a reader and a writer.
    fn split(self) -> io::Result<(StreamReader<Self>, StreamWriter<Self>)> {
        let reader = StreamReader(self.try_clone()?);
        let writer = StreamWriter(self);
        Ok((reader, writer))
    }
}

/// Reader part of a stream.
pub struct StreamReader<S: Stream>(S);

impl<S: Stream> Drop for StreamReader<S> {
    fn drop(&mut self) {
        self.0.shutdown(Shutdown::Read).unwrap_or(());
    }
}

impl<S: Stream> io::Read for StreamReader<S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }

    fn read_vectored(&mut self, bufs: &mut [io::IoSliceMut<'_>]) -> io::Result<usize> {
        self.0.read_vectored(bufs)
    }

    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        self.0.read_to_end(buf)
    }

    fn read_to_string(&mut self, buf: &mut String) -> io::Result<usize> {
        self.0.read_to_string(buf)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.0.read_exact(buf)
    }
}

/// Writer part of a stream.
pub struct StreamWriter<S: Stream>(S);

impl<S: Stream> Drop for StreamWriter<S> {
    fn drop(&mut self) {
        self.0.shutdown(Shutdown::Write).unwrap_or(());
    }
}

impl <S: Stream> io::Write for StreamWriter<S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn write_vectored(&mut self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        self.0.write_vectored(bufs)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl Stream for TcpStream {
    fn try_clone(&self) -> io::Result<Self> {
        self.try_clone()
    }

    fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.set_nonblocking(nonblocking)
    }

    fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        self.shutdown(how)
    }
}

#[cfg(unix)]
impl Stream for UnixStream {
    fn try_clone(&self) -> io::Result<Self> {
        self.try_clone()
    }

    fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        self.set_nonblocking(nonblocking)
    }

    fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        self.shutdown(how)
    }
}
