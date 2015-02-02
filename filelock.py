#!/usr/bin/env python

"""Module for advisory locking of files on disk."""

from __future__ import division, absolute_import, print_function, unicode_literals

import os
import io
import sys
import copy
import time
import fcntl
import logging
# import datetime

_log_name = 'main' if __name__ == '__main__' else __name__
log = logging.getLogger(_log_name)
log.setLevel(logging.DEBUG if _log_name is 'main' else logging.ERROR)

# Clumsy fix for py3k compatibility
# TODO: find a more elegant solution for the bastring issue in py3k
try:
    unicode
except NameError:
    basestring = (str, bytes)


class PosixLock(io.IOBase):

    """Simple class for creating on disk lock files on POSIX Operating Systems.

    When used as a context manager, an object of this class will automatically
    lock and unlock itself. Also, it can be set up to automatically delete its
    associated disk file upon closing.

    This class must be inherited to be useful. It should be inherited BEFORE a
    file-like class (ideally something in the io.IOBase hierarchy). Regardless
    of the class type, the class MUST implement a fileno() method returning a
    file descriptor. This is because all the locking is done at the operating
    system level using file descriptors.
    """

    def __init__(self, *args, **kwargs):
        """Initialize a PosixLock object.

        :param block: Whether or not to block when attempting to lock the file
        when another lock is in place already. This defaults to True. If set to
        False, locking will throw an IOError if the file is already locked. See
        fcntl module for more documentation. This parameter must be passed as a
        keyword argument.
        :param delete: Whether or not to delete the PosixLock's associated disk
        file when it is closed. This defaults to False to give the behaviour
        most people would expect when working with file objects. This parameter
        must be passed as a keyword argument.
        """
        self.block = bool(kwargs.pop('block', True))
        self.delete = bool(kwargs.pop('delete', False))
        self.log = logging.getLogger(_log_name + '.PosixLock')

        super(PosixLock, self).__init__(*args, **kwargs)

    def lock(self, block=None):
        """Lock the owned disk file.

        :param block: If 'block' is set to 'None' then the blocking setting
        will be taken from the objects 'block' attribute. Else, 'block' will be
        cast to a bool. When block is set to False, an OSError exception is
        raised if the the file is already locked by a different process. When
        block is set to True (the object level default), then this method will
        block until the file can be locked by this object, at which point the
        method will return.
        """
        self.log.debug("Attempting lock on object '%s'...", repr(self))

        try:
            op = copy.copy(self.lock_op)
        except AttributeError:
            if '+' in self.mode or 'w' in self.mode or 'a' in self.mode:
                self.lock_op = fcntl.LOCK_EX
            elif 'r' in self.mode:
                self.lock_op = fcntl.LOCK_SH
            op = copy.copy(self.lock_op)

        block = self.block if block is None else bool(block)

        if block is False:  # if in non-blocking mode
            op |= fcntl.LOCK_NB

        try:
            fcntl.lockf(self, op)
        except OSError:
            self.log.debug("Exception thrown for attempted lock on object %r!",
                           self)
            raise

        self.log.debug("Lock acquired for object %s!", repr(self))

    def unlock(self):
        """If the disk file is locked by this object, then unlock it.

        This should have no effect if the file is already unlocked.
        """
        self.log.debug("Attempting to unlock object '%s'...", repr(self))
        fcntl.lockf(self, fcntl.LOCK_UN)
        self.log.debug("Unlocking complete for object %s!", repr(self))

    def close(self, delete=None):
        """Close the file.

        This will behave like the close method on any other builtin file object
        with one exception: if delete is set to True at the object level or as
        a method parameter, the object will attempt to unlink the file from
        disk before closing (this is allowed on Unix-like systems).
        """
        if not self.closed:
            delete = self.delete if delete is None else bool(delete)

            if delete:
                try:
                    os.unlink(self.name)
                except OSError:
                    pass

        return super(PosixLock, self).close()

    def __enter__(self):
        """Enter context."""
        self.lock()
        return super(PosixLock, self).__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit context."""
        return super(PosixLock, self).__exit__(exc_type, exc_value, traceback)


class FileIO(PosixLock, io.FileIO):
    pass


class BufferedReader(PosixLock, io.BufferedReader):
    pass


class BufferedWriter(PosixLock, io.BufferedWriter):
    pass


class BufferedRandom(PosixLock, io.BufferedRandom):
    pass


class TextIOWrapper(PosixLock, io.TextIOWrapper):
    pass

# The current implementation of 'open' is copied nearly verbatim from the
# Python 2.6 io.py implementation. This will probably be re-implemented in
# the future to avoid potential licensing issues.
# TODO: Create an original implementation of "open" function
def open(file, mode="r", buffering=None, encoding=None, errors=None,
         newline=None, closefd=True, block=True, delete=False):
    r"""Open file and return a stream. If the file cannot be opened, an IOError is raised."""
    if not isinstance(file, (basestring, int)):
        raise TypeError("invalid file: %r" % file)
    if not isinstance(mode, basestring):
        raise TypeError("invalid mode: %r" % mode)
    if buffering is not None and not isinstance(buffering, int):
        raise TypeError("invalid buffering: %r" % buffering)
    if encoding is not None and not isinstance(encoding, basestring):
        raise TypeError("invalid encoding: %r" % encoding)
    if errors is not None and not isinstance(errors, basestring):
        raise TypeError("invalid errors: %r" % errors)
    modes = set(mode)
    if modes - set("arwb+tU") or len(mode) > len(modes):
        raise ValueError("invalid mode: %r" % mode)
    reading = "r" in modes
    writing = "w" in modes
    appending = "a" in modes
    updating = "+" in modes
    text = "t" in modes
    binary = "b" in modes
    if "U" in modes:
        if writing or appending:
            raise ValueError("can't use U and writing mode at once")
        reading = True
    if text and binary:
        raise ValueError("can't have text and binary mode at once")
    if reading + writing + appending > 1:
        raise ValueError("can't have read/write/append mode at once")
    if not (reading or writing or appending):
        raise ValueError("must have exactly one of read/write/append mode")
    if binary and encoding is not None:
        raise ValueError("binary mode doesn't take an encoding argument")
    if binary and errors is not None:
        raise ValueError("binary mode doesn't take an errors argument")
    if binary and newline is not None:
        raise ValueError("binary mode doesn't take a newline argument")
    raw = FileIO(file,
                 (reading and "r" or "") +
                 (writing and "w" or "") +
                 (appending and "a" or "") +
                 (updating and "+" or ""),
                 closefd, block=block, delete=delete)
    if buffering is None:
        buffering = -1
    line_buffering = False
    if buffering == 1 or buffering < 0 and raw.isatty():
        buffering = -1
        line_buffering = True
    if buffering < 0:
        buffering = io.DEFAULT_BUFFER_SIZE
        try:
            bs = os.fstat(raw.fileno()).st_blksize
        except (os.error, AttributeError):
            pass
        else:
            if bs > 1:
                buffering = bs
    if buffering < 0:
        raise ValueError("invalid buffering size")
    if buffering == 0:
        if binary:
            return raw
        raise ValueError("can't have unbuffered text I/O")
    if updating:
        buffer = BufferedRandom(raw, buffering, block=block, delete=delete)
    elif writing or appending:
        buffer = BufferedWriter(raw, buffering, block=block, delete=delete)
    elif reading:
        buffer = BufferedReader(raw, buffering, block=block, delete=delete)
    else:
        raise ValueError("unknown mode: %r" % mode)
    if binary:
        return buffer
    text = TextIOWrapper(buffer, encoding, errors, newline, line_buffering,
                         block=block, delete=delete)
    text.mode = mode
    return text


# TODO: use subprocess or multiprocessing module to test this, instead of
# manually starting two processes.
def _test(**kwargs):
    """Test file locking classes."""
    logging.basicConfig(level=logging.DEBUG)
    print("Opening file...")
    fp = open("/tmp/test.lock", **kwargs)
    fp.lock()
    if len(sys.argv) > 1:
        print("Gonna go to sleep for a while.")
        time.sleep(5)
        print("Ok. Done sleeping.")
        fp.unlock()
    else:
        print("I will get here when the lock is released by another process.")
        fp.unlock()
    fp.close()

    with open("/tmp/test_with.lock", **kwargs) as wfp:
        wfp.flush()
        time.sleep(5)

if __name__ == '__main__':
    _test(mode='wb', buffering=0)
    _test(mode='rb')
    _test(mode='r+b')
