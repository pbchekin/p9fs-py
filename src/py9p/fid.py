MIN_FID = 1024
MAX_FID = 65535
IOUNIT = 1024 * 16


class NoFidError(Exception):
    pass


class FidCache(dict):
    """
    Fid cache class

    The class provides API to acquire next not used Fid
    for the 9p operations. If there is no free Fid available,
    it raises NoFidError(). After usage, Fid should be freed
    and returned to the cache with release() method.
    """
    def __init__(self, start=MIN_FID, limit=MAX_FID):
        """
         * start -- the Fid interval beginning
         * limit -- the Fid interval end

        All acquired Fids will be from this interval.
        """
        dict.__init__(self)
        self.start = start
        self.limit = limit
        self.iounit = IOUNIT
        self.fids = list(range(self.start, self.limit + 1))

    def acquire(self):
        """
        Acquire next available Fid
        """
        if len(self.fids) < 1:
            raise NoFidError()
        return Fid(self.fids.pop(0), self.iounit)

    def release(self, f):
        """
        Return Fid to the free Fids queue.
        """
        self.fids.append(f.fid)


class Fid(object):
    """
    Fid class

    It is used also in the stateful I/O, representing
    the open file. All methods, working with open files,
    will receive Fid as the last parameter.

    See: write(), read(), release()
    """
    def __init__(self, fid, iounit=IOUNIT):
        self.fid = fid
        self.iounit = iounit
