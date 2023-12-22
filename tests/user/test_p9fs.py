"""Tests fsspec functions with an actual real 9p server.

This test suite requires a locally running 9p server that exports an empty directory.

    TMPDIR=$(mktemp -d)

Examples:
    unpfs 'tcp!0.0.0.0!1234' $TMPDIR
    python -m py9p -p 1234 -w -r $TMPDIR

Pass this TMPDIR to the test suite with `--exported`:
    python -m pytest tests/user --exported $TMPDIR

"""

import pathlib

import fsspec
import pytest

import p9fs


@pytest.fixture(scope='session')
def fs(pytestconfig):
    return p9fs.P9FileSystem(
        host=pytestconfig.getoption('--host'),
        port=pytestconfig.getoption('--port'),
        username=pytestconfig.getoption('--user'),
        version=pytestconfig.getoption('--9p'),
        verbose=pytestconfig.getoption('--chatty'),
    )


@pytest.fixture(scope='session')
def exported_path(pytestconfig):
    path = pathlib.Path(pytestconfig.getoption('--exported'))
    test_path = path / 'xxx'
    with test_path.open('w', encoding='utf-8') as test_file:
        test_file.write('This is a test content')
    return path


def test_exists(fs):
    assert not fs.exists('no_such_file_or_dir')


def test_mkdir(fs):
    assert not fs.exists('test_mkdir')
    fs.mkdir('test_mkdir', create_parents=False)
    assert fs.isdir('test_mkdir')

    assert not fs.exists('test_mkdir/dir1')
    fs.mkdir('test_mkdir/dir11', create_parents=False)
    assert fs.isdir('test_mkdir/dir11')

    assert not fs.exists('test_mkdir/dir2/dir21')
    fs.mkdir('test_mkdir/dir2/dir21', create_parents=True, exist_ok=True)
    assert fs.isdir('test_mkdir/dir2/dir21')


def test_makedirs(fs):
    assert not fs.exists('test_makedirs/dir1/dir11')
    fs.makedirs('test_makedirs/dir1/dir11', exist_ok=True)
    assert fs.isdir('test_makedirs/dir1/dir11')

    assert not fs.exists('test_makedirs/dir2/dir21/')
    fs.makedirs('test_makedirs/dir22/dir21/', exist_ok=True)
    assert fs.isdir('test_makedirs/dir22/dir21/')


def test_rmdir(fs):
    assert not fs.exists('test_rmdir/dir1/dir11')
    fs.makedirs('test_rmdir/dir1/dir11', exist_ok=True)
    assert fs.isdir('test_rmdir/dir1/dir11')
    fs.rmdir('test_rmdir/dir1/dir11')
    assert not fs.exists('test_rmdir/dir1/dir11')


def test_info(fs):
    assert not fs.exists('test_info')
    fs.makedirs('test_info/dir1/dir11', exist_ok=True)
    info = fs.info('test_info/dir1')
    assert info['name'] == 'test_info/dir1/'
    assert info['type'] == 'directory'

    info = fs.info('test_info/dir1/')
    assert info['name'] == 'test_info/dir1/'
    assert info['type'] == 'directory'


def test_ls(fs):
    assert not fs.exists('test_ls')
    fs.makedirs('test_ls/dir1', exist_ok=True)
    items = fs.ls('test_ls', detail=False)
    assert len(items) == 1
    assert 'test_ls/dir1/' in items

    fs.makedirs('test_ls/dir2', exist_ok=True)
    fs.makedirs('test_ls/dir3', exist_ok=True)
    items = fs.ls('test_ls', detail=False)
    assert len(items) == 3
    assert all(item in items for item in ['test_ls/dir1/', 'test_ls/dir2/', 'test_ls/dir3/'])

    items = fs.ls('test_ls/dir1', detail=False)
    assert len(items) == 0

    items = fs.ls('test_ls', detail=True)
    assert len(items) == 3
    assert all(item in [info['name'] for info in items] for item in ['test_ls/dir1/', 'test_ls/dir2/', 'test_ls/dir3/'])


def test_read(fs, exported_path):
    with fs.open('xxx', mode='r') as f:
        assert f.read() == 'This is a test content'


def test_write(fs, exported_path):
    with fs.open('test_write', mode='w') as f:
        f.write('Written by py9p')

    with (exported_path / 'test_write').open(mode='r') as f:
        assert f.read() == 'Written by py9p'

    with fs.open('test_write_big', mode='w') as f:
        for line in range(8192):
            f.write(f'{line} Written by py9p\n')

    with (exported_path / 'test_write_big').open(mode='r') as f:
        assert len(f.readlines()) == 8192


def test_cp_file(fs, exported_path):
    assert not fs.exists('test_cp_file')
    fs.mkdir('test_cp_file')

    fs.cp_file('xxx', 'test_cp_file/xxx')
    assert fs.isfile('test_cp_file/xxx')


def test_copy(fs, exported_path):
    assert not fs.exists('test_copy')
    fs.mkdir('test_copy')

    fs.copy('xxx', 'test_copy/')
    assert fs.isfile('test_copy/xxx')

    fs.copy('xxx', 'test_copy/yyy')
    assert fs.isfile('test_copy/yyy')

    if fs.isfile('yyy'):
        fs.rm('yyy')

    fs.copy('test_copy/yyy', '.')
    assert fs.isfile('yyy')


def test_registration(fs, exported_path):
    url = f'p9://{fs.username}@{fs.host}:{fs.port}/xxx?version={fs.version.value}'
    with fsspec.open(url, 'r') as f:
        data = f.read()
        assert data == 'This is a test content'
