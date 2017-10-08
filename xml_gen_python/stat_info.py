#!/usr/bin/python
#coding=utf-8

g_stat = {

        "line_count": 0,
        "file_count": 0,
        "record_count": 0,
        "record_count": 0,
        "good_record_count": 0,
        "bad_record_count": 0,
        "commandid_count": 0,
        "fragment_count": 0,
        "redis_count": 0,
        }

def dump_status(stat):
    print "处理文件总数:%s"%stat['file_count'];

class statInfo:
    input_path=None;
    line_count= 0;
    file_count= 0;
    record_count= 0;
    record_count= 0;
    good_record_count= 0;
    bad_record_count= 0;
    commandid_count= 0;
    commandid_dict={}
    commandid_len_dict={}
    redis_last_time=0;

    def __init(self):
        self.line_count= 0;
        self.file_count= 0;
        self.record_count= 0;
        self.good_record_count= 0;
        self.bad_record_count= 0;
        self.commandid_count= 0;
        self.commandid_dict={};
        self.commandid_len_dict={};
        self.redis_last_time=time.time();

    def dump(self):
        print "处理文件总数:%s"%self.file_count;
        print "坏记录总数:%s"%self.bad_record_count;
        print "记录总数:%s"%self.record_count;
        print "分片总数:%s"%self.fragment_count;
        print "剩余未能凑成一段,记录数:%s"%self.redis_count;
        print "commandid:%s"%self.commandid;
        if len(self.commandid_dict.keys()) < 256:
            print "commandid_dict:%s"%self.commandid_dict;
        else:
            print "commandid_dict 超过256个, 确认一下 分隔符是否有问题, commandid 不会超过256个";
            print "随机commandid  取样如下";
            for each in range(1,8):
                print  self.commandid_dict.popitem();
            
        print "commandid_len_dict:%s"%self.commandid_len_dict;
        print "input_path:%s"%self.input_path;
        
          
if __name__ == '__main__':
    a=statInfo();
    for  each in xrange(1,100):
        a.file_count+=1;
    a.dump();
