"""9P implementation of fsspec."""

from __future__ import annotations

import errno
import io
import os
import pathlib
import socket
import stat
import threading
from typing import Optional, List, Dict, Union

import fsspec.spec
from fsspec import utils

from py9p import fid
from py9p import py9p

Version = py9p.Version


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

    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: Optional[str] = None,
        version: Union[str, Version] = Version.v9P2000L,
        verbose: bool = False,
        aname: str = '',
        **kwargs,
    ):
        """9P implementation of fsspec.

        Args:
            host: 9P server host
            port: 9P server port
            username: 9P username
            password: 9P password
            version: one of '9P2000', '9P2000.u', '9P2000.L', or Version
        """
        super().__init__(**kwargs)
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        if isinstance(version, str):
            self.version = Version.from_str(version)
        else:
            self.version = version
        self.verbose = verbose
        self.aname = aname
        self.fids = fid.FidCache()
        self._rlock = threading.RLock()
        super().__init__(**kwargs)
        self._connect()

    @classmethod
    def _strip_protocol(cls, path):
        path = utils.infer_storage_options(path)["path"]
        return path.lstrip('/')

    @staticmethod
    def _get_kwargs_from_urls(path):
        # Example: 9p://nobody@host:port/directory/file.csv
        options = utils.infer_storage_options(path)
        options.pop('path', None)
        options.pop('protocol', None)
        url_query = options.pop('url_query')
        if url_query:
            for item in url_query.split('&'):
                key, value = item.split('=')
                options[key] = value
        return options

    def _connect(self):
        s = socket.socket(socket.AF_INET)
        s.connect((self.host, self.port))
        credentials = py9p.Credentials(user=self.username, passwd=self.password)
        self.client = py9p.Client(
            s,
            credentials=credentials,
            ver=self.version,
            chatty=self.verbose,
            aname=self.aname,
        )

    def _mkdir(self, path, mode: int = 0o755):
        self._mknod(path, mode | stat.S_IFDIR)

    @with_fid
    def _mknod(self, tfid, path, mode):
        parts = pathlib.Path(path).parts
        self.client._walk(self.client.ROOT, tfid, parts[:-1])
        if self.version == Version.v9P2000L:
            if mode & stat.S_IFDIR:
                self.client._mkdir(tfid, parts[-1], py9p.mode2plan(mode))
            else:
                self.client._lcreate(tfid, parts[-1], os.O_TRUNC | os.O_CREAT | os.O_WRONLY, mode, 0)
        else:
            self.client._create(tfid, parts[-1], py9p.mode2plan(mode), 0)
        self.client._clunk(tfid)

    @with_fid
    def _info(self, tfid, path):
        parts = pathlib.Path(path).parts
        response = self.client._walk(self.client.ROOT, tfid, parts)
        # canonical 9p implementation and specifically diod do not return an error if walk is
        # unsuccessful. Instead, they return all qids up to last existing component in the path.
        # The only way to check if path exists is compare the number of qids in the response.
        if len(response.wqid) != len(parts):
            raise P9FileNotFound(path)
        if self.version == Version.v9P2000L:
            response = self.client._getattr(tfid)
            self.client._clunk(tfid)
            return self._info_from_rgetattr(path, response)
        else:
            response = self.client._stat(tfid)
            self.client._clunk(tfid)
            info = self._info_from_rstat(str(pathlib.Path(path).parent), response)
            if len(info) != 1:
                raise P9Error(f'stat returned {len(info)} items instead of 1')
            return info[0]

    def _info_from_rgetattr(self, path: str, response) -> Dict:
        item = response.stat[0]
        qid = response.qid
        node_type = 'directory' if qid.type & py9p.QTDIR else 'file'
        if node_type == 'directory' and not path.endswith('/'):
            path = f'{path}/'
        return {
            'name': path,
            'type': node_type,
            'mode': py9p.mode2stat(item.mode),
            'size': item.length,
            'atime': item.atime,
            'mtime': item.mtime,
            'ctime': item.ctime,
        }

    def _info_from_rstat(self, parent: str, response) -> List[Dict]:
        """Transforms 9P stat response to a list of fsspec info"""
        items = []
        for item in response.stat:
            node_type = 'directory' if item.mode & py9p.DMDIR else 'file'
            name = item.name.decode('utf-8')
            full_name = f'{parent}{name}' if parent.endswith('/') else f'{parent}/{name}'
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
        # TODO: diod does not support unlinkat, remove should be used instead.
        # Currently the only way to identify diod is to check if aname is set.
        if self.version == Version.v9P2000L and not self.aname:
            p = pathlib.Path(path)
            self.client._walk(self.client.ROOT, tfid, p.parent.parts)
            self.client._unlinkat(tfid, p.name)
        else:
            parts = pathlib.Path(path).parts
            self.client._walk(self.client.ROOT, tfid, parts)
            self.client._remove(tfid)

    @with_fid
    def _lsdir(self, tfid, path):
        """Lists dir entries for 9P2000/9P2000.u"""
        parts = pathlib.Path(path).parts
        self.client._walk(self.client.ROOT, tfid, parts)
        try:
            if self.client._open(tfid, 0) is None:
                return []
            items = []
            offset = 0
            while True:
                response = self.client._read(tfid, offset, self.client.msize)
                buffer = response.data
                if len(buffer) == 0:
                    break
                p9 = py9p.Marshal9P(dotu=self.version == Version.v9P2000u)
                p9.setBuffer(buffer)
                p9.buf.seek(0)
                response = py9p.Fcall(py9p.Rstat)
                p9.decstat(response.stat, 0)
                items.extend(self._info_from_rstat(path, response))
                offset += len(buffer)
            return items
        finally:
            self.client._clunk(tfid)

    @with_fid
    def _readdir(self, tfid, path):
        """Lists dir entries for 9P2000.L"""
        p = pathlib.Path(path)
        self.client._walk(self.client.ROOT, tfid, p.parts)
        try:
            response = self.client._lopen(tfid, py9p.OREAD)
            if response is None:
                return []
            items = []
            offset = 0
            while True:
                response = self.client._readdir(tfid, offset, self.client.msize - py9p.IOHDRSZ)
                if response.count == 0:
                    break
                for entry in response.stat:
                    if entry.name == '.' or entry.name == '..':
                        continue
                    name = str(p / entry.name)
                    if entry.qid.type & py9p.QTDIR:
                        name = f'{name}/'
                    items.append(name)
                offset = response.stat[-1].offset
            return items
        finally:
            self.client._clunk(tfid)

    @with_fid
    def _open_fid(self, tfid, path, mode):
        f = self.fids.acquire()
        try:
            parts = pathlib.Path(path).parts
            self.client._walk(self.client.ROOT, f.fid, parts)
            if self.version == Version.v9P2000L:
                fcall = self.client._lopen(f.fid, mode)
            else:
                fcall = self.client._open(f.fid, py9p.open2plan(mode))
            if fcall.iounit == 0:
                f.iounit = self.client.msize
            else:
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
            if len(error.args) > 0 and (error.args[0] == b'file not found' or error.args[0] == errno.ENOENT):
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

        if self.version == Version.v9P2000L:
            items = self._readdir(path)
            return items if not detail else [self.info(item) for item in items]
        else:
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
            for i in range((src_info['size'] + self.client.msize - 1) // self.client.msize):
                block = self._read(self.client.msize, i * self.client.msize, sf)
                self._write(block, i * self.client.msize, df)
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

    def created(self, path):
        return self.info(path).get('ctime')

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

        data = self.buffer.getvalue()
        size = len(data)
        offset = 0
        msize = self.fs.client.msize - py9p.IOHDRSZ
        while offset < size - 1:
            asize = min(msize, size - offset)
            self.fs._write(data[offset:offset + asize], self.offset + offset, self._f)
            offset += asize
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
        data = io.BytesIO()
        offset = start
        msize = self.fs.client.msize - py9p.IOHDRSZ
        while offset < end:
            asize = min(msize, end - offset + 1)
            offset += data.write(self.fs._read(asize, offset, self._f))
        return data.getvalue()

    def close(self):
        """Close file"""
        super().close()
        self.fs._release_fid(self._f)
