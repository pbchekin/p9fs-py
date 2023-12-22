# p9fs

9P implementation of Python fsspec.

Supported protocols:

* [9P2000](https://ericvh.github.io/9p-rfc/rfc9p2000.html)
* [9P2000.u](https://ericvh.github.io/9p-rfc/rfc9p2000.u.html)
* [9P2000.L](https://github.com/chaos/diod/blob/master/protocol.md)

Supported servers:

* py9p
* unpfs

TODO:
* `atime`, `mtime`, `ctime`

This package contains a fork of py9p (https://github.com/svinota/py9p), which seems no longer maintained.
