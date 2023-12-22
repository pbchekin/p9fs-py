# p9fs

9P implementation of Python fsspec.

Supported protocols:

* [9P2000](https://ericvh.github.io/9p-rfc/rfc9p2000.html)
* [9P2000.u](https://ericvh.github.io/9p-rfc/rfc9p2000.u.html)
* [9P2000.L](https://github.com/chaos/diod/blob/master/protocol.md)

Supported servers:

* [py9p](https://github.com/pbchekin/p9fs-py/blob/main/src/py9p/__main__.py)
* [unpfs](https://github.com/pfpacket/rust-9p/blob/master/README.md#unpfs)

## Examples

```python
import p9fs

fs = p9fs.P9FileSystem(
    host='127.0.0.1',
    port=564,
    username='nobody',
)

print(fs.ls('.'))
```

## TODO

* `auth`
* `atime`, `mtime`, `ctime`

This package contains a fork of py9p (https://github.com/svinota/py9p), which seems no longer maintained.
Minimal support for 9P2000.L has been added to the client code.
