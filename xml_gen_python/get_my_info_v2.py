# -*- coding:utf8 -*-

import argparse
parser = argparse.ArgumentParser()

parser.add_argument('--input_path', action='store', dest='input_path', help='Store a simple value', default='/root/data/dbinfo0506/')

parser.add_argument('--touch', action='store', dest='touch', type=bool, default=False,  help='touch file ')

parser.add_argument('--once', action='store', dest='once', type=bool, default=False,  help='run once ')

parser.add_argument('--micro_second', action='store', dest='micro_second', default=False,  help='micro_second  ')

parser.add_argument('--delete_file', action='store', dest='delete_file', default=False,  help='delete_file file ')

parser.add_argument('--array_count', action='store', type=int, dest='array_count', default=16,  help='array_count file ')


parser.add_argument('--split_str', action='store', type=str, dest='split_str', default='<!!@!!>',  help='split_str file ')

parser.add_argument('--commandid', action='append', dest='commandid', default=[], help='Add repeated values to a command')

parser.add_argument('--version', action='version', version='%(prog)s 1.0')

options = parser.parse_args()
print options;

#print '--input_path     =', options.input_path
#print '--touch     =', options.touch
#print '--commandid     =', options.commandid
#print '--delete_file     =', options.delete_file

#if options.touch:
#    print "touch file ";

