import glob
import os;
import time;
for file in glob.glob("/root/data/dbinfo0506/*"):
    if os.path.isfile(file):

        cur_time=time.time();
        mtime = os.path.getmtime(file)   
        diff=cur_time - mtime ;

        if diff < 5:
            print "File:   {0} ->  with   {1}  seconds ".format(file, diff);
