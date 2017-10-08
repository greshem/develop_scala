import glob
import os;


g_count=0;
for each in glob.glob("/root/data/dbinfo0506/*.db"):
    count= len(open(each).readlines());
    print "%s->%d"%(each, count);
    g_count+=count;

print "count=%s"%g_count;
    
