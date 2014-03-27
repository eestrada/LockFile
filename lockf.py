#!/usr/bin/python
from __future__ import division, absolute_import, with_statement, print_function, unicode_literals

import os
import io
import sys
import time
import fcntl
import logging
import datetime

log = logging.getLogger(__name__ + '.LockFile')
log.setLevel(logging.DEBUG)

class LockFile(io.TextIOWrapper):
    """Simple class for creating on disk lock files on POSIX Operating Systems
    
    When used in a context manager, an object of this class will automatically lock and unlock itself. Also, it can be set up to automatically delete its associated disk file upon closing.

    This class inherits from io.TextIOWrapper. Relevant documentation on parameters and methods not found here, can be found in the documentation for that class.
    
    Since this class inherits from io.TextIOWrapper, ALL input must be unicode strings. These will then be written to disk with the encoding specified in the constructor."""

    def __init__(self, file, mode='a', encoding='utf8', errors='replace', newline=None, line_buffering=True, block=True, delete=False):
        """Create a LockFile object

        :param file: Path to a file on disk. LockFile should work correctly on NFS volumes if the NFS is set up to support locking, and the OS that is locking the file supports NFS locking as well. Check the relavent documentation of your Operating System and NFS set up.
        :param mode: mode may be 'w', 'w+', 'a', 'a+', or 'r+'. Opening a LockFile for read-only (i.e. mode value of 'r' alone) means it cannot be locked; this is a common OS level restriction. Thus, LockFile will reject it outright  by raising a ValueError exception instead of giving incorrect behaviour, which would be more difficult to debug.
        :param encoding: LockFile objects are ALWAYS opened as a text file, never as a raw byte streams. Thus they need to know how to encode text data when writing to the file. This defaults to 'utf8'.
        :param errors: How to deal with encoding and decoding errors. Defaults to 'replace'.
        :param newline: Control how line endings are handled. Defaults to None.
        :param line_buffering: Whether or not to flush buffered data to disk when a newline character is encountered. Defaults to True.
        :param delete: Whether or not to delete the LockFile's associated disk file when it is closed. This defaults to False to give the behaviour most people would expect when working with file objects."""

        self.delete = bool(delete)
        self.block = bool(block)
        raw = io.FileIO(file, mode=mode)
        if '+' in mode:
            buff_file = io.BufferedRandom(raw)
        elif 'w' in mode or 'a' in mode:
            buff_file = io.BufferedWriter(raw)
        else:
            raise ValueError("'mode' parameter does not contain acceptable values: %s" % mode )

        super(LockFile, self).__init__(buff_file, encoding=encoding, 
            errors=errors, newline=newline, line_buffering=line_buffering)

    def lock(self, block=None):
        """Lock the owned disk file
        
        :param block: If 'block' is set to 'None' then the blocking setting will be taken from the objects 'block' attribute. Else, 'block' will be cast to a bool. When block is set to False, an OSError exception is raised if the the file is already locked by a different process. When block is set to True (the object level default), then this method will block until the file can be locked by this object, at which point the method will return."""

        log.debug("Obtaining lock for object '%s'...", repr(self))
        op = fcntl.LOCK_EX

        block = self.block if block == None else bool(block)

        if block is False: # if in non-blocking mode
            op |= fcntl.LOCK_NB

        fcntl.lockf(self, op)

        log.debug("Lock acquired for object %s!", repr(self))

    def unlock(self):
        """If the disk file is locked by this object, then unlock it
        
        This should have no effect if the file is already unlocked."""

        log.debug("Attempting to unlock object '%s'...", repr(self))
        fcntl.lockf(self, fcntl.LOCK_UN)
        log.debug("Unlocking complete for object %s!", repr(self))

    def close(self, delete=None):
        """Close the file
        
        This will behave like the close method on any other builtin file object with one exception: if delete is set to True at the object level or as a method parameter, the object will attempt to immediatly unlink the file from disk after closing."""

        super(LockFile, self).close()

        delete = self.delete if delete == None else bool(delete)

        if delete and not self.closed:
            try:
                os.unlink(self.name)
            except OSError:
                pass

    def __enter__(self):
        self.lock()
        return super(LockFile, self).__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        return super(LockFile, self).__exit__(exc_type, exc_value, traceback)

def _test():
    logging.basicConfig(level=logging.DEBUG)
    print("Opening file...")
    fp = LockFile("/tmp/test.lock", "a")
    fp.lock()
    if len(sys.argv) > 1:
        print("Gonna go to sleep for a while.")
        time.sleep(5)
        print("Ok. Done sleeping.")
        fp.unlock()
    else:
        print("I will get here when the lock is released from another process.")
        fp.unlock()
    fp.write("I was here.\n")
    fp.close()

    with LockFile("/tmp/test_with.lock", "a") as wfp:
        wfp.write('is test file working? I hope so.\n')
        wfp.flush()
        time.sleep(5)

if __name__ == '__main__':
    _test()

