"""9P implementation of fsspec."""

from __future__ import annotations

import os
import pathlib
import socket
import stat
import threading
from typing import Optional, List, Dict

import fsspec.spec

from py9p import fid
from py9p import py9p


def with_fid(m):
    """Decorator for P9FileSystem methods to provide fids.

    Acquires a temporary fid before the operation and releases it after.
    """
    def wrapped(self, *argv, **kwarg):
        tfid = self.fids.acquire()
        with self._rlock:
            return m(self, tfid.fid, *argv, **kwarg)
    return wrapped


class P9Error(Exception):
    """Base class for P9FileSystem errors."""


class P9FileNotFound(P9Error):
    """File not found error."""


class P9FileSystem(fsspec.AbstractFileSystem):
    """9P implementation of fsspec."""

    protocol = '9p'

    host: str
    port: int
    username: Optional[str]
    password: Optional[str]
    msize: int = 8192

    def __init__(
            self,
            host: str,
            port: int = py9p.PORT,
            username: Optional[str] = None,
            password: Optional[str] = None,
            **kwargs,
    ):
        """9P implementation of fsspec."""
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.fids = fid.FidCache()
        self._rlock = threading.RLock()
        super().__init__(**kwargs)
        self._connect()

    def _connect(self):
        s = socket.socket(socket.AF_INET)
        s.connect((self.host, self.port))
        credentials = py9p.Credentials(user=self.username, passwd=self.password)
        self.client = py9p.Client(s, credentials=credentials)

    def _mkdir(self, path, mode: int = 0o755):
        self._mknod(path, mode | stat.S_IFDIR)

    @with_fid
    def _mknod(self, tfid, path, mode):
        parts = pathlib.Path(path).parts
        self.client._walk(self.client.ROOT, tfid, parts[:-1])
        self.client._create(tfid, parts[-1], py9p.mode2plan(mode), 0)
        self.client._clunk(tfid)

    @with_fid
    def _info(self, tfid, path):
        parts = pathlib.Path(path).parts
        self.client._walk(self.client.ROOT, tfid, parts)
        response = self.client._stat(tfid)
        self.client._clunk(tfid)
        info = self._info_from_response(str(pathlib.Path(path).parent), response)
        if len(info) != 1:
            raise P9Error(f'stat returned {len(info)} items instead of 1')
        return info[0]

    def _info_from_response(self, parent: str, response) -> List[Dict]:
        """Transforms 9P stat response to a list of fsspec info"""
        items = []
        for item in response.stat:
            node_type = 'directory' if item.mode & py9p.DMDIR else 'file'
            name = item.name.decode('utf-8')
            if parent.endswith('/'):
                full_name = f'{parent}{name}'
            else:
                full_name = f'{parent}/{name}'
            if node_type == 'directory':
                full_name = f'{full_name}/'
            items.append(
                {
                    'name': full_name,
                    'type': node_type,
                    'mode': py9p.mode2stat(item.mode),
                    'size': item.length,
                    'atime': item.atime,
                    'mtime': item.mtime,
                }
            )
        return items

    @with_fid
    def _unlink(self, tfid, path):
        parts = pathlib.Path(path).parts
        self.client._walk(self.client.ROOT, tfid, parts)
        self.client._remove(tfid)

    @with_fid
    def _lsdir(self, tfid, path):
        parts = pathlib.Path(path).parts
        self.client._walk(self.client.ROOT, tfid, parts)
        try:
            if self.client._open(tfid, 0) is None:
                return []
            items = []
            offset = 0
            while True:
                response = self.client._read(tfid, offset, self.msize)
                buffer = response.data
                if len(buffer) == 0:
                    break
                p9 = py9p.Marshal9P()
                p9.setBuffer(buffer)
                p9.buf.seek(0)
                response = py9p.Fcall(py9p.Rstat)
                p9.decstat(response.stat, 0)
                items.extend(self._info_from_response(path, response))
                offset += len(buffer)
            return items
        finally:
            self.client._clunk(tfid)


    @with_fid
    def _open_fid(self, tfid, path, mode):
        f = self.fids.acquire()
        try:
            parts = pathlib.Path(path).parts
            self.client._walk(self.client.ROOT, f.fid, parts)
            fcall = self.client._open(f.fid, py9p.open2plan(mode))
            f.iounit = fcall.iounit
            return f
        except Exception as e:
            self.fids.release(f)
            raise e

    @with_fid
    def _release_fid(self, tfid, f: py9p.Fid):
        if f is None:
            return
        try:
            self.client._clunk(f.fid)
            self.fids.release(f)
        except py9p.RpcError:
            pass

    def _write(self, buf, offset, f):
        size = len(buf)
        for i in range((size + f.iounit - 1) // f.iounit):
            start = i * f.iounit
            length = start + f.iounit
            self.client._write(f.fid, offset + start, buf[start:length])
        return size

    def _read(self, size, offset, f):
        data = bytes()
        while True:
            # we do not rely nor on msize, neither on iounit,
            # so, shift offset only with real data read
            ret = self.client._read(f.fid, offset, min(size - len(data), f.iounit))
            data += ret.data
            offset += len(ret.data)
            if size <= len(data) or len(ret.data) == 0:
                break
        return data[:size]

    def mkdir(self, path, create_parents=True, exist_ok=False, mode: int = 0o755, **kwargs):
        if create_parents:
            self.makedirs(path, exist_ok=exist_ok)
            return
        self._mkdir(path=path, mode=mode)

    def makedirs(self, path, exist_ok=False):
        parents = pathlib.Path(path).parents
        # skip '.' item returned by 'parents'
        items = [str(parent) for parent in reversed(parents) if str(parent) != '.']
        items.append(path)
        for item in items:
            if exist_ok and self.exists(item):
                if not self.isdir(item):
                    raise P9Error(f'{item} exists and not a directory')
                continue
            self.mkdir(item, create_parents=False)

    def info(self, path, **kwargs):
        try:
            return self._info(path=path)
        except py9p.RpcError as error:
            if len(error.args) > 0 and error.args[0] == b'file not found':
                raise P9FileNotFound(path) from error
            raise error

    def exists(self, path, **kwargs):
        """Is there a file at the given path"""
        try:
            self.info(path, **kwargs)
            return True
        except P9FileNotFound:
            return False

    def isdir(self, path):
        """Is this entry directory-like?"""
        try:
            return self.info(path)['type'] == 'directory'
        except P9FileNotFound:
            return False

    def isfile(self, path):
        """Is this entry file-like?"""
        try:
            return self.info(path)['type'] == 'file'
        except P9FileNotFound:
            return False

    def rmdir(self, path):
        self._unlink(path)

    def ls(self, path, detail=True, **kwargs):
        info = self.info(path)
        if info['type'] != 'directory':
            return info if detail else info['name']

        items = self._lsdir(path)
        return items if detail else [item['name'] for item in items]

    def _rm(self, path):
        self._unlink(path)

    def cp_file(self, path1, path2, **kwargs):
        """Copy within two locations in the same filesystem."""
        src_info = self.info(path1)
        dst_info = None
        try:
            dst_info = self.info(path2)
        except P9FileNotFound:
            self._mknod(path2, src_info['mode'])
            dst_info = self.info(path2)

        if dst_info['type'] == 'directory':
            path2 = str(pathlib.Path(path2) / pathlib.Path(path1).name)
            self._mknod(path2, src_info['mode'])

        sf = self._open_fid(path1, os.O_RDONLY)
        df = self._open_fid(path2, os.O_WRONLY | os.O_TRUNC)
        try:
            for i in range((src_info['size'] + self.msize - 1) // self.msize):
                block = self._read(self.msize, i * self.msize, sf)
                self._write(block, i * self.msize, df)
        finally:
            self._release_fid(sf)
            self._release_fid(df)

    def mv(self, path1, path2, recursive=None, maxdepth=None, **kwargs):
        if path1 == path2:
            return
        self.copy(path1, path2, recursive=recursive, maxdepth=maxdepth)
        self.rm(path1, recursive=recursive)

    def modified(self, path):
        return self.info(path)['mtime']

    def _open(
            self,
            path,
            mode="rb",
            block_size=None,
            autocommit=True,
            cache_options=None,
            **kwargs,
    ):
        return P9BufferedFile(
            self,
            path,
            mode,
            block_size,
            autocommit,
            cache_options=cache_options,
            **kwargs,
        )


class P9BufferedFile(fsspec.spec.AbstractBufferedFile):

    _f: Optional[py9p.Fid] = None

    def _upload_chunk(self, final=False):
        """Write one part of a multi-block file upload

        Parameters
        ==========
        final: bool
            This is the last block, so should complete file, if
            self.autocommit is True.
        """
        if self.offset is None or self._f is None:
            try:
                self._initiate_upload()
            except:  # noqa: E722
                self.closed = True
                raise

        self.fs._write(self.buffer.getvalue(), self.offset, self._f)
        return True

    def _initiate_upload(self):
        """Create remote file/upload"""
        self.offset = 0
        self.fs._mknod(self.path, 0o755)
        self._f = self.fs._open_fid(self.path, os.O_WRONLY | os.O_TRUNC)

    def _fetch_range(self, start, end):
        """Get the specified set of bytes from remote"""
        if self._f is None:
            self._f = self.fs._open_fid(self.path, os.O_RDONLY)
        return self.fs._read(end - start + 1, start, self._f)

    def close(self):
        """Close file"""
        super().close()
        self.fs._release_fid(self._f)
