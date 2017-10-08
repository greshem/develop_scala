#! /usr/bin/env python 
#coding: utf-8 
import time;
from tools import gen_xml_file


def logger(string):
    fh = open("/tmp/consume_one_chinese_str.py.log", 'a')
    fh.write("%s: %s\n"%(get_cur_time(), string) )
    fh.close();





import sys
reload(sys);
sys.setdefaultencoding("utf-8")  
import redis 

#sys.path.append("../"); 
#import ../smplayer_mp3.py 
#from   smplayer_mp3 import say_chinese_old

def redis_gen_xml_from_queue(r,splitter):
    lines=[];
    for each in range(0, splitter):
        queue,info=r.brpop("lines");
        if  not info.endswith("\n"):
            info+="\n" 
        lines.append(info);
    ret=gen_xml_file(lines, redis=True);
    return ret;

  
if __name__ == '__main__':
    r = redis.client.Redis(host='127.0.0.1', port=6379, db=1)
    while 1:
        splitter=2048;
        ret=r.llen("lines");
        if ret >splitter:
            print "count=%s  > %s"%(ret,splitter);
            info=redis_gen_xml_from_queue(r,splitter);
            

        else:
            print "less then   1024 , skip ";
        time.sleep(1);
