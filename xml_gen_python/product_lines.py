#!/usr/bin/python
#coding=utf-8

import shutil;
import time;
import glob;
import sys, os
from  tools import  gen_xml_file;
import redis 
from stat_info import  *;
from consume_lines  import *;

#global options;
g_stat=statInfo();


#52520<!!@@!!>10

#commandid:  houseid:  gathertime:  srcip:  destip:  srcport:  destport:  domainname:  proxytype:  proxyip:  proxyport:  title:  content:  url: atname
#52285|10077|1496566943|217.182.132.65|58.211.8.22|56330|80|cul.dmmap.com||||民族服饰特点之德昂族服饰|百度|http://cul.dmmap.com/cul_dmwh_msfq/cul_dmwh_msfq_tsfs/20111201/0000000033ed92b50133f7fcc87d0182.html|0x01+0x01e1+000++IDC+021+20170604170223+440.txt|END

def stat_array(array):

    if g_stat.commandid_len_dict.get(len(array))==None:
       g_stat.commandid_len_dict[len(array)]=1;
    else:
       g_stat.commandid_len_dict[len(array)]+=1;


    if g_stat.commandid_dict.get(array[0])==None:
       g_stat.commandid_dict[array[0]]=1;
    else:
       g_stat.commandid_dict[array[0]]+=1;

    if  len(array)!=options.array_count:
        g_stat.bad_record_count+=1;


def gen_xml_line(array):
    g_stat.record_count+=1;
    #print "|".join(array);
    #commandid:  houseid:  gathertime:  srcip:  destip:  srcport:  destport:  domainname:  proxytype:  proxyip:  proxyport:  title:  content:  url: atname
    array = [ each.replace("<<<!>>>","")   for each in   array ];

    stat_array(array);

    output_str="""<log><logId>{logId}</logId><commandId>{commandId}</commandId><houseId>{houseId}</houseId><srcIp>{srcIp}</srcIp><destIp>{destIp}</destIp><srcPort>{srcPort}</srcPort><destPort>{destPort}</destPort><domain>{domain}</domain><proxyType>{proxyType}</proxyType><proxyIp>{proxyIp}</proxyIp><proxyPort>{proxyPort}</proxyPort><title>{title}</title><content>{content}</content><url>{url}</url><attachment><title>{title}</title><file>{file}</file></attachment><gatherTime>{gatherTime}</gatherTime></log>\n""";


    if  len(array)!=options.array_count:
        return  output_str;
    ret=output_str.format(logId=0, 
            commandId=array[0],
            houseId=array[1],
            gatherTime=array[2], 
            srcIp=  array[3],
            destIp= array[4],
            srcPort=array[5],
            destPort=array[6],
            domain=array[7],
            proxyType=array[8],
            proxyIp=array[9],
            proxyPort=array[10],
            title=   array[11],
            content= array[12],
            url=     array[13],
            file=   "file");
    return ret;


#fh=open("data/172.17.48.174-01e0.20170604170315.139742152746752.00000.db");
def get_data_from_file(file,commandid=None):
    if commandid!=None:
        assert(isinstance(commandid, str) );

    ret=[];

    #fh=open("/root/data/dbinfo0506/172.17.80.32-01e0.20170505105633.140105926833920.00000.db");
    fh=open(file);
    for line in fh.readlines():
        #array=line.split('<!!@@!!>')
	if  options.split_str==",":
            line=line.replace("<<<!>>>,","<<<!>>>,,,");
            array=line.split(",,,")
	else:
            array=line.split(options.split_str)

    	array = [ each.replace("<<<!>>>","")   for each in   array ];
        assert(isinstance(array[0], str));
        if commandid==None:
            ret.append(array);
        else:
            if array[0] in  commandid:
                ret.append(array);
    return ret; 


def get_least_5_second_files(input_dir):
    ret=[];
    for file in glob.glob(input_dir):
        if os.path.isfile(file):
            cur_time=time.time();
            mtime = os.path.getmtime(file)   
            diff=cur_time - mtime ;

            if diff < 5:
                ret.append(file);
                #print "File:   {0} ->  with   {1}  seconds ".format(file, diff);
    return ret;

def watch_dir_db_ok_list(input_dir):
    ret=[];
    assert(input_dir[-1]=="/");
    dbfile_pat="%s*.db"%input_dir;
    for file in glob.glob(dbfile_pat):
        if os.path.isfile(file) and  os.path.isfile(file+".ok"):
            ret.append(file);
    return ret;



def watch_dir_list(input_path):
    #least_5_file=get_least_5_second_files("/root/data/dbinfo0506/*db")
    assert(os.path.isdir(input_path));
    least_5_file=get_least_5_second_files("%s/*.db"%input_path)
    print "File_list :%s"%(least_5_file);
    return least_5_file;



def process_file_list(file_list, commandid):
    import redis 
    r = redis.client.Redis(host='127.0.0.1', port=6379, db=1)

    

    data=[];
    count=0;
    for each in  file_list:
        print "load:%s %s"%(count,each);
        data.extend(get_data_from_file(each,commandid));
        count+=1;

    print "LEN:%s"%len(data);
    #2048分片:
    count=len(data)/2048
    g_stat.fragment_count=count;

    for each in range(0,count):
        print ( "%s -> %s "%(each*2048, (each+1)*2048 ) );
        gen_xml_file(map(gen_xml_line, data[each*2048:(each+1)*2048  ] ));

    g_stat.redis_count=len(data[(count)*2048: ]);
    for each in data[(count)*2048: ]:
        #print "||||||%s"%each;
        line= gen_xml_line(each);
        r.lpush("lines",  line  );

    ret=r.llen("lines");
    print "内存队列 length %d "%(ret);
    #r.close();

def chomp(line):
    if line[-1] == '\n':
        line = line[:-1]
    return line;


def delete_files(file_list, input_path):
    if len(file_list) >0:
    	assert(file_list[0].startswith(input_path));
    if not os.path.isdir(input_path+"/delete/"):
	os.mkdir(input_path+"/delete/");
    for each in file_list:
	print "  move %s -> %s"%(each, input_path+"/delete/");
	shutil.move(each, input_path+"/delete/");
	print "  move %s.ok -> %s"%(each, input_path+"/delete/");
	shutil.move(each+".ok", input_path+"/delete/");

if __name__ == '__main__':
    from  get_my_info import  * ;
    #from  get_my_info_v2 import  * ;

    #global options
    print options;
    input_path=options.input_path;
    commandid=options.commandid;

    g_stat.input_path=input_path;
    g_stat.commandid=commandid;

    if options.touch:
        os.system("touch  %s/*"%input_path);


    file_list=None;
    if os.path.isfile(input_path):
        file_list=map( chomp, open(input_path).readlines());
    elif  os.path.isdir(input_path):
        #file_list = watch_dir_list(input_path);
        file_list = watch_dir_db_ok_list(input_path);
    else:
        print "%s is error \n"%(input_path);


    #只运行一次:
    print options.once;
 
    if options.once :
        process_file_list(file_list, commandid);
        g_stat.file_count=len(file_list);
        if options.delete_file:
            delete_files(file_list, input_path);
        g_stat.dump();
    else:
        while [ 1 ]:
            print "#=========================";
            file_list = watch_dir_db_ok_list(input_path);
            process_file_list(file_list, commandid);
            if options.delete_file:
                delete_files(file_list, input_path)
            g_stat.dump();

            #清空redis
            cur_time=time.time();
            r = redis.client.Redis(host='127.0.0.1', port=6379, db=1)
            ret=r.llen("lines");
            if cur_time >  g_stat.redis_last_time + 60 and    ret > 0 :
                print "内存队列 可以清空 生成文件了"
                info=redis_gen_xml_from_queue(r,ret);
                g_stat.redis_last_time = cur_time;
            #r.close()
            time.sleep(5);
