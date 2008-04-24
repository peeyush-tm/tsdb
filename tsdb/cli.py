#!/usr/bin/env python

import os
import sys
import time
from cmd import Cmd
from optparse import OptionParser
from pprint import pprint

from tsdb import *

VERSION="0.37"

class TSDBCLI(Cmd):
    """Implements a simple CLI for inspecting TSDBs"""

    def __init__(self, db_path):
        Cmd.__init__(self)

        self.db_path = db_path
        self.db_name = os.path.basename(self.db_path)
        self.prompt = 'tsdb[%s]> ' % self.db_name
        self.intro = 'TSDB v%s' % VERSION

        self.db = TSDB(db_path)
        self.pwl = self.db

        self._hist = []
        self._locals = {}
        self._globals = {}

    def do_quit(self, args):
        return 1
        self.do_quit(args)

    do_q = do_quit
    do_EOF = do_quit

    def do_list_sets(self, args):
        # XXX need to handle paths
        for set in self.pwl.list_sets():
            print set

    def do_list_vars(self, args):
        # XXX need to handle paths
        for set in self.pwl.list_vars():
            print set
    do_lv = do_list_vars

    def do_list_all(self, arg):
        self.do_list_sets(arg)
        self.do_list_vars(arg)

    do_ls = do_list_all

    def do_cl(self, arg):
        # XXX need to handle paths
        newloc = None

        if arg == "..":
            newloc = self.pwl.parent
        else:
            try:
                newloc = self.pwl.get_set(arg)
            except TSDBSetDoesNotExistError:
                try:
                    newloc = self.pwl.get_var(arg)
                except TSDBVarDoesNotExistError:
                    try:
                        newloc = self.pwl.get_aggregate(arg)
                    except TSDBAggregateDoesNotExistError:
                        print "unable to change to location: %s" % arg
        if newloc:
            self.pwl = newloc

    def complete_cl(self, text, line, begidx, endidx):
        comp = []

        def has_completion(text, poss_comp):
            comp = []
            for item in poss_comp:
                if item.startswith(text):
                    comp.append(item)
            return comp

        comp.extend(has_completion(text, self.pwl.list_sets()))
        comp.extend(has_completion(text, self.pwl.list_vars()))
        comp.extend(has_completion(text, self.pwl.list_aggregates()))

        return comp

    do_cd = do_cl
    complete_cd = complete_cl


    def do_pwl(self, arg):
        print self.pwl

    do_pwd = do_pwl

    #def do_metadata(self, arg):
    #    print self.pwl.metadata

    def do_get(self, arg):
        print self.pwl.get(int(arg))

    def do_dir(self, arg):
        print dir(self.pwl)

    def do_type(self, arg):
        if isinstance(self.pwl, TSDBVar):
            print self.pwl.tag, self.pwl.type.__name__
        else:
            print self.pwl.tag

    def do_select(self, arg):
        if not isinstance(self.pwl, TSDBVar):
            print "Not a var:", self.pwl
            return

        args = arg.split()
        d = {}

        if len(args) > 0:
            for a in args:
                (k,v) = a.split('=')
                d[k] = int(v)
        else:
            d['end'] = time.time()
            d['begin'] = d['end'] - self.pwl.metadata['STEP'] * 25

        for row in self.pwl.select(**d):
            print time.ctime(row.timestamp), row

    def default(self, arg):
        args = arg.split()
        if hasattr(self.pwl, args[0]):
            attr = getattr(self.pwl, args[0])
            if callable(attr):
                if isinstance(self.pwl, TSDBVar):
                    if args[0] == 'get':
                        print attr(int(args[1]))
                    elif args[0] == 'update_aggregate':
                        print attr(args[1])
                    elif args[0] == 'update_all_aggregates':
                        print attr()
                else:
                    print "add feature to call method", args[0]
            else:
                pprint(attr)

def main():
    parser = OptionParser(usage="%prog [options] DATABASE", version="%prog "+VERSION)

    (options, args) = parser.parse_args()

    if len(args) != 1:
        print >>sys.stderr, "must specify database"
        sys.exit()

    db_path = args[0]
    TSDBCLI(db_path).cmdloop()

if __name__ == '__main__':
    main()
