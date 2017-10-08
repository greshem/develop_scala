# -*- coding:utf8 -*-

import os
import datetime
import sys
from optparse import OptionParser
#from optparse.OptionParser  import * ;

def   parse_args():
    try:
        usage = """usage: %prog [options] arg1 arg2 
        python  product_lines.py  --input_path  input_path           --once 1 
        python  product_lines.py  --input_path  input_path/delete/   --once 1 

#==========================================================================
"""
        opt = OptionParser(usage=usage);
        opt.add_option('--input_path',
                       dest='input_path',
                       type=str,
                       default='/root/data/dbinfo0506/',
                       help='/root/data/dbinfo0506/')

        opt.add_option('--touch',
                       dest='touch',
                       type=int,
                       default=0,
                       help='touch path')


        opt.add_option('--delete_file',
                       dest='delete_file',
                       type=int,
                       default=0,
                       help='delete_file')


        opt.add_option('--mirco_second',
                       dest='mirco_second',
                       type=int,
                       default=1,
                       help='mirco_second')

        opt.add_option('--once',
                       dest='once',
                       #action="store_false",
                       type=int,
                       default=0,
                       help='run one ')

        opt.add_option('--commandid',
                       dest="commandid",
                       type=str,
                       default=None,
                       help="commandid")

        opt.add_option('--array_count',
                       dest="array_count",
                       type=int,
                       default=16,
                       help="array_count")


        opt.add_option('--split_str',
                       dest="split_str",
                       type=str,
                       default='<!!@@!!>',
                       help="split_str")



        (options, args) = opt.parse_args()
        print args;
        print options;
        return options;
    except Exception as ex:
        print("exception :{0}".format(str(ex)))

global options;
options= parse_args();

if __name__ == '__main__':
    options= parse_args();
    print options;

