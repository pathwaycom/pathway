use std::os::unix::prelude::*;

use std::io;

use cfg_if::cfg_if;
use nix::fcntl::{fcntl, FcntlArg, FdFlag, OFlag};
use nix::unistd;

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub enum ReaderType {
    Blocking,
    NonBlocking,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub enum WriterType {
    Blocking,
    NonBlocking,
}

#[derive(Debug)]
pub struct Pipe {
    pub reader: OwnedFd,
    pub writer: OwnedFd,
}

impl Pipe {
    unsafe fn from_raw_fds((reader, writer): (RawFd, RawFd)) -> Self {
        let reader = unsafe { OwnedFd::from_raw_fd(reader) };
        let writer = unsafe { OwnedFd::from_raw_fd(writer) };
        Self { reader, writer }
    }
}

fn set_non_blocking(fd: impl AsFd) -> io::Result<()> {
    let fd = fd.as_fd();
    let flags = fcntl(fd.as_raw_fd(), FcntlArg::F_GETFL)?;
    let flags = unsafe { OFlag::from_bits_unchecked(flags) };
    fcntl(fd.as_raw_fd(), FcntlArg::F_SETFL(flags | OFlag::O_NONBLOCK))?;
    Ok(())
}

#[allow(dead_code)] // it is used on non-Linux
fn set_cloexec(fd: impl AsFd) -> io::Result<()> {
    let fd = fd.as_fd();
    let flags = fcntl(fd.as_raw_fd(), FcntlArg::F_GETFD)?;
    let flags = unsafe { FdFlag::from_bits_unchecked(flags) };
    fcntl(
        fd.as_raw_fd(),
        FcntlArg::F_SETFD(flags | FdFlag::FD_CLOEXEC),
    )?;
    Ok(())
}

pub fn pipe(reader_type: ReaderType, writer_type: WriterType) -> io::Result<Pipe> {
    cfg_if! {
        if #[cfg(target_os = "linux")] {
            let pipe = unistd::pipe2(OFlag::O_CLOEXEC)?;
            let pipe = unsafe { Pipe::from_raw_fds(pipe) };
        } else {
            let pipe = unistd::pipe()?;
            let pipe = unsafe { Pipe::from_raw_fds(pipe) };
            set_cloexec(&pipe.reader)?;
            set_cloexec(&pipe.writer)?;
        }
    }

    if let ReaderType::NonBlocking = reader_type {
        set_non_blocking(&pipe.reader)?;
    }

    if let WriterType::NonBlocking = writer_type {
        set_non_blocking(&pipe.writer)?;
    }

    Ok(pipe)
}
