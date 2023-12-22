import pytest

import p9fs


def test_from_string():
    v = p9fs.Version.from_str('9P2000')
    assert v == p9fs.Version.v9P2000

    v = p9fs.Version.from_str('9P2000.u')
    assert v == p9fs.Version.v9P2000u

    v = p9fs.Version.from_str('9P2000.L')
    assert v == p9fs.Version.v9P2000L

    with pytest.raises(KeyError):
        p9fs.Version.from_str('no_such_version')


def test_to_bytes():
    assert p9fs.Version.v9P2000.to_bytes() == b'9P2000'
