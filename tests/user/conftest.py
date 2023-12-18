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
        help=f"Directory exported by 9p server, default: /tmp",
    )
