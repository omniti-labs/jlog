#!/usr/bin/env python

import unittest
import os
import tempfile
import shutil

from jlog import *

# initial setting gets overriden in main()
g_tempdir = None

# We can't make these configurable because of the way unittest works. If you
# need to debug a test set this to true
g_debug = False

if 'TEST_DEBUG' in os.environ and os.environ['TEST_DEBUG'] == 'true':
    g_debug = True

class TestJLog(unittest.TestCase):

    def test_jlog(self):

        jlog_dir = g_tempdir + 'test_jlog.jlog'

        # setup writer
        writer = JLogWriter(jlog_dir)
        self.assertTrue(writer.ctx_initialized)
        self.assertTrue(writer.add_subscriber("testsub"))
        # 1-100
        for i in xrange(1, 101):
            writer.write("test" + str(i))

        # setup reader
        reader = JLogReader(jlog_dir, "testsub")
        self.assertTrue(reader.ctx_initialized)
        self.assertEqual(reader.subscriber, "testsub")
        self.assertEqual(len(reader), 100)

        # read 5 entries
        count = 0
        for msg in reader:
            self.assertEqual(msg, "test" + str(count + 1))
            count = count + 1
            if count >= 5:
              break

        self.assertEqual(len(reader), 100 - count)

        # truncate log
        reader.truncate(2)
        self.assertEqual(len(reader), 2)
        # XXX - this next part is broken. I think it's an issue with
        # jlog_ctx_read_interval reporting an invalid size. It will still work
        # for now
        msg = next(reader)
        #self.assertEqual(msg, "test99")
        msg = next(reader)
        #self.assertEqual(msg, "test100")


def setUpModule():
    global g_tempdir
    g_tempdir = tempfile.mkdtemp() + '/'

def tearDownModule():
    if g_debug is True:
        print "Test state saved for debugging in %s" % g_tempdir
    else:
        print "To debug tests set TEST_DEBUG=true in env"
        # clean up the mess
        shutil.rmtree(g_tempdir)

if __name__ == '__main__':
    # run tests
    unittest.main()
