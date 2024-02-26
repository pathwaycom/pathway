// Copyright © 2024 Pathway

use std::io;
use std::os::fd::{AsFd, AsRawFd, OwnedFd};

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

fn set_non_blocking(fd: impl AsFd) -> io::Result<()> {
    let fd = fd.as_fd();
    let flags = fcntl(fd.as_raw_fd(), FcntlArg::F_GETFL)?;
    let flags = OFlag::from_bits_retain(flags);
    fcntl(fd.as_raw_fd(), FcntlArg::F_SETFL(flags | OFlag::O_NONBLOCK))?;
    Ok(())
}

#[cfg_attr(target_os = "linux", allow(dead_code))]
fn set_cloexec(fd: impl AsFd) -> io::Result<()> {
    let fd = fd.as_fd();
    let flags = fcntl(fd.as_raw_fd(), FcntlArg::F_GETFD)?;
    let flags = FdFlag::from_bits_retain(flags);
    fcntl(
        fd.as_raw_fd(),
        FcntlArg::F_SETFD(flags | FdFlag::FD_CLOEXEC),
    )?;
    Ok(())
}

pub fn pipe(reader_type: ReaderType, writer_type: WriterType) -> io::Result<Pipe> {
    cfg_if! {
        if #[cfg(target_os = "linux")] {
            let (reader, writer) = unistd::pipe2(OFlag::O_CLOEXEC)?;
        } else {
            let (reader, writer) = unistd::pipe()?;
            set_cloexec(&reader)?;
            set_cloexec(&writer)?;
        }
    }

    if let ReaderType::NonBlocking = reader_type {
        set_non_blocking(&reader)?;
    }

    if let WriterType::NonBlocking = writer_type {
        set_non_blocking(&writer)?;
    }

    Ok(Pipe { reader, writer })
}
