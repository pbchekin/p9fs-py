import os


def pytest_addoption(parser):
    parser.addoption(
        "--host",
        action="store",
        default='127.0.0.1',
        type=str,
        help="9p server host, default: 127.0.0.1",
    )
    parser.addoption(
        "--port",
        action="store",
        default=1234,
        type=int,
        help="9p server port, default: 1234",
    )
    parser.addoption(
        "--user",
        action="store",
        default=os.getenv('USER'),
        type=str,
        help=f"9p server port, default: {os.getenv('USER')}",
    )
    parser.addoption(
        "--exported",
        action="store",
        default="/tmp",
        type=str,
        help="Directory exported by 9p server, default: /tmp",
    )
    parser.addoption(
        "--9p",
        action="store",
        default="9P2000",
        type=str,
        help="9P version: 9P2000 (default), 9P2000.u, 9P2000.L",
    )
    parser.addoption(
        "--chatty",
        action="store_true",
        default=False,
        help="Verbose 9P client",
    )
    parser.addoption(
        "--aname",
        action="store",
        default="",
        type=str,
        help="Name to attach to, required for diod",
    )
