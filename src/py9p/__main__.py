#!/usr/bin/env python

# Copyright (c) 2008-2011 Tim Newsham, Andrey Mirtchovski
# Copyright (c) 2011-2012 Peter V. Saveliev
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import sys
import stat
import os.path
import pwd
import grp
import getopt
import getpass

from py9p import py9p


def _os(func, *args):
    try:
        return func(*args)
    except OSError as e:
        raise py9p.ServerError(e.args)
    except IOError as e:
        raise py9p.ServerError(e.args)


def _nf(func, *args):
    try:
        return func(*args)
    except py9p.ServerError:
        return


def uidname(u):
    try:
        return "%s" % pwd.getpwuid(u).pw_name
    except KeyError:
        return "%d" % u


def gidname(g):
    try:
        return "%s" % grp.getgrgid(g).gr_name
    except KeyError:
        return "%d" % g


class LocalFs(object):
    """
    A local filesystem device.
    """

    files = {}

    def __init__(self, root, cancreate=0, dotu=0):
        self.dotu = dotu
        self.cancreate = cancreate
        self.root = self.pathtodir(root)
        self.root.parent = self.root
        self.root.localpath = root
        self.files[self.root.qid.path] = self.root

    def getfile(self, path):
        if path not in self.files:
            return None
        return self.files[path]

    def pathtodir(self, f):
        '''Stat-to-dir conversion'''
        s = _os(os.lstat, f)
        u = uidname(s.st_uid)
        g = gidname(s.st_gid)
        res = s.st_mode & 0o777
        type = 0
        ext = ""
        if stat.S_ISDIR(s.st_mode):
            type = type | py9p.QTDIR
            res = res | py9p.DMDIR
        qid = py9p.Qid(type, 0, py9p.hash8(f))
        if self.dotu:
            if stat.S_ISLNK(s.st_mode):
                res = py9p.DMSYMLINK
                ext = os.readlink(f)
            elif stat.S_ISCHR(s.st_mode):
                ext = "c %d %d" % (os.major(s.st_rdev), os.minor(s.st_rdev))
            elif stat.S_ISBLK(s.st_mode):
                ext = "b %d %d" % (os.major(s.st_rdev), os.minor(s.st_rdev))
            else:
                ext = ""

            return py9p.Dir(1, 0, s.st_dev, qid,
                            res,
                            int(s.st_atime), int(s.st_mtime),
                            s.st_size, os.path.basename(f), u, gidname(s.st_gid), u,
                            ext, s.st_uid, s.st_gid, s.st_uid)
        else:
            return py9p.Dir(0, 0, s.st_dev, qid,
                            res,
                            int(s.st_atime), int(s.st_mtime),
                            s.st_size, os.path.basename(f), u, g, u)

    def open(self, srv, req):
        f = self.getfile(req.fid.qid.path)
        s = _os(os.lstat, f.localpath)
        if not f:
            srv.respond(req, "unknown file")
            return
        if (req.ifcall.mode & 3) == py9p.OWRITE:
            if not self.cancreate:
                srv.respond(req, "read-only file server")
                return
            if req.ifcall.mode & py9p.OTRUNC:
                m = "wb"
            else:
                m = "r+b"        # almost
        elif (req.ifcall.mode & 3) == py9p.ORDWR:
            if not self.cancreate:
                srv.respond(req, "read-only file server")
                return
            if m & py9p.OTRUNC:
                m = "w+b"
            else:
                m = "r+b"
        else:                # py9p.OREAD and otherwise
            m = "rb"
        if not (f.qid.type & py9p.QTDIR) and not stat.S_ISLNK(s.st_mode):
            f.fd = _os(open, f.localpath, m)
        srv.respond(req, None)

    def walk(self, srv, req):
        f = self.getfile(req.fid.qid.path)
        if not f:
            srv.respond(req, 'unknown file')
            return
        npath = f.localpath
        for path in req.ifcall.wname:
            # normpath takes care to remove '.' and '..', turn '//' into '/'
            npath = os.path.normpath(npath + "/" + path)
            if len(npath) <= len(self.root.localpath):
                # don't let us go beyond the original root
                npath = self.root.localpath

            if path == '.' or path == '':
                req.ofcall.wqid.append(f.qid)
            elif path == '..':
                # .. resolves to the parent, cycles at /
                qid = f.parent.qid
                req.ofcall.wqid.append(qid)
                f = f.parent
            else:
                try:
                    d = self.pathtodir(npath)
                except:
                    srv.respond(req, "file not found")
                    return

                nf = self.getfile(d.qid.path)
                if nf:
                    # already exists, just append to req
                    req.ofcall.wqid.append(d.qid)
                    f = nf
                else:
                    d.localpath = npath
                    d.basedir = "/".join(npath.split("/")[:-1])
                    d.parent = f
                    self.files[d.qid.path] = d
                    req.ofcall.wqid.append(d.qid)
                    f = d

        req.ofcall.nwqid = len(req.ofcall.wqid)
        srv.respond(req, None)

    def remove(self, srv, req):
        f = self.getfile(req.fid.qid.path)
        if not f:
            srv.respond(req, 'unknown file')
            return
        if not self.cancreate:
            srv.respond(req, "read-only file server")
            return

        if f.qid.type & py9p.QTDIR:
            _os(os.rmdir, f.localpath)
        else:
            _os(os.remove, f.localpath)
        self.files[req.fid.qid.path] = None
        srv.respond(req, None)

    def create(self, srv, req):
        fd = None
        if not self.cancreate:
            srv.respond(req, "read-only file server")
            return
        if req.ifcall.name == '.' or req.ifcall.name == '..':
            srv.respond(req, "illegal file name")
            return

        f = self.getfile(req.fid.qid.path)
        if not f:
            srv.respond(req, 'unknown file')
            return
        name = f.localpath + '/' + req.ifcall.name
        if req.ifcall.perm & py9p.DMDIR:
            perm = req.ifcall.perm & (~0o777 | (f.mode & 0o777))
            _os(os.mkdir, name, req.ifcall.perm & ~(py9p.DMDIR))
        elif req.ifcall.perm & py9p.DMSYMLINK and self.dotu:
            _os(os.symlink, req.ifcall.extension, name)
        else:
            perm = req.ifcall.perm & (~0o666 | (f.mode & 0o666))
            _os(open, name, "w+").close()
            _os(os.chmod, name, perm)
            if (req.ifcall.mode & 3) == py9p.OWRITE:
                if req.ifcall.mode & py9p.OTRUNC:
                    m = "wb"
                else:
                    m = "r+b"        # almost
            elif (req.ifcall.mode & 3) == py9p.ORDWR:
                if m & py9p.OTRUNC:
                    m = "w+b"
                else:
                    m = "r+b"
            else:                # py9p.OREAD and otherwise
                m = "rb"
            fd = _os(open, name, m)

        d = self.pathtodir(name)
        d.parent = f
        self.files[d.qid.path] = d
        self.files[d.qid.path].localpath = name
        self.files[d.qid.path].basedir = "/".join(name.split("/")[:-1])
        if fd:
            self.files[d.qid.path].fd = fd
        req.ofcall.qid = d.qid
        srv.respond(req, None)

    def clunk(self, srv, req):
        f = self.getfile(req.fid.qid.path)
        if not f:
            srv.respond(req, 'unknown file')
            return
        f = self.files[req.fid.qid.path]
        if hasattr(f, 'fd') and f.fd is not None:
            f.fd.close()
            f.fd = None
        srv.respond(req, None)

    def stat(self, srv, req):
        f = self.getfile(req.fid.qid.path)
        if not f:
            srv.respond(req, "unknown file")
            return
        req.ofcall.stat.append(self.pathtodir(f.localpath))
        srv.respond(req, None)

    def wstat(self, srv, req):

        istat = req.ifcall.stat[0]
        f = self.getfile(req.fid.qid.path)
        if (istat.uidnum >> 16) == 0xFFFF:
            istat.uidnum = -1
        if (istat.gidnum >> 16) == 0xFFFF:
            istat.gidnum = -1

        _os(os.chown, f.localpath, istat.uidnum, istat.gidnum)
        # change mode?
        if istat.mode != 0xFFFFFFFF:
            s = _os(os.lstat, f.localpath)
            imode = s.st_mode
            mode = ((imode & 0o7777) ^ imode) | \
                   (istat.mode & 0o7777)
            _os(os.chmod, f.localpath, mode)
        # change name?
        if istat.name:
            _os(os.rename, f.localpath, "/".join((f.basedir,
                                                  istat.name.decode('utf-8'))))
        srv.respond(req, None)

    def read(self, srv, req):
        f = self.getfile(req.fid.qid.path)
        s = _os(os.lstat, f.localpath)
        if not f:
            srv.respond(req, "unknown file")
            return

        if stat.S_ISLNK(s.st_mode) and self.dotu:
            d = self.pathtodir(f.localpath)
            req.ofcall.data = d.extension
        elif f.qid.type & py9p.QTDIR:
            # no need to add anything to self.files yet
            # wait until they walk to it
            l = _os(os.listdir, f.localpath)
            l = filter(lambda x: x not in ('.', '..'), l)
            req.ofcall.stat = []
            for x in l:
                req.ofcall.stat.append(self.pathtodir(f.localpath + '/' + x))
        else:
            f.fd.seek(req.ifcall.offset)
            req.ofcall.data = f.fd.read(req.ifcall.count)
        srv.respond(req, None)

    def write(self, srv, req):
        if not self.cancreate:
            srv.respond(req, "read-only file server")
            return

        f = self.getfile(req.fid.qid.path)
        if not f:
            srv.respond(req, "unknown file")
            return

        f.fd.seek(req.ifcall.offset)
        f.fd.write(req.ifcall.data)
        req.ofcall.count = len(req.ifcall.data)
        srv.respond(req, None)


def usage(prog):
    print("usage:  %s [-dDw] [-c mode] [-p port] [-r root] " \
          "[-a address] [srvuser [domain]]" % prog)
    sys.exit(1)


def main():
    prog = sys.argv[0]
    args = sys.argv[1:]

    port = py9p.PORT
    listen = '0.0.0.0'
    root = '/'
    user = None
    chatty = 0
    cancreate = 0
    dotu = 0
    authmode = None
    dom = None
    passwd = None
    key = None

    try:
        opt, args = getopt.getopt(args, "dDwp:r:a:c:")
    except:
        usage(prog)
    for opt, optarg in opt:
        if opt == "-d":
            chatty = 1
        if opt == "-D":
            dotu = 1
        if opt == '-w':
            cancreate = 1
        if opt == '-r':
            root = optarg
        if opt == "-p":
            port = int(optarg)
        if opt == '-a':
            listen = optarg
        if opt == '-c':
            authmode = optarg

    if authmode == 'pki':
        try:
            py9p.pki = __import__("py9p.pki").pki
            user = 'admin'
        except:
            import traceback
            traceback.print_exc()
    elif authmode is not None and authmode != 'none':
        print("unknown auth type: %s; accepted: pki, sk1, none" % authmode)
        sys.exit(1)

    srv = py9p.Server(listen=(listen, port),
                      authmode=authmode,
                      user=user,
                      dom=dom,
                      key=key,
                      chatty=chatty,
                      dotu=dotu)
    srv.mount(LocalFs(root, cancreate, dotu))
    srv.serve()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("interrupted.")
