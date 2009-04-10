import os, os.path

def setup():
    tmpdir = os.environ.get('TMPDIR', 'tmp')
    if not os.path.exists(tmpdir):
        os.makedirs(tmpdir)

