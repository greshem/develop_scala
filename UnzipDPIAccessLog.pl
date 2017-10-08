#!/usr/local/bin/perl
######################### UnzipDPIAccessLog.pl ##########################
# 文件类型：功能模块shell源程序
# 基本功能：dpi机房日志解压程序
# 开发平台：SUN solaris
# 编    者：xuebing
# 完成日期：2013.08.30
# 任务号/BUG号：5644/N
# 概要/详设文档/变更号：2013082401－dpi支持机房基础监测功能－张志龙－5644.doc
# 20160504,chengfeng,TD21479/BUG35018,进程判重异常，-w 完全匹配进程号
# 20160308,chengfeng,N/N,访问日志优化
# 20160229,chengfeng,N/N,将UnzipDPIAccessLog.pl和UnzipOneLog_put_2_hdfs.pl合并,另外若程序一直卡住,不做超时退出处理,增加日志。
#########################################################################
use strict;

use GetSHDir;
use Getopt::Long;
use Time::Local;
use POSIX qw(setsid);
use POSIX qw(:errno_h :sys_wait_h);

#通用全局变量
my $APPNAME = 'UnzipDPIAccessLog.pl';

#1、SetupDir
my $SetupDir = GetSHDir('SetupDir');
if ($SetupDir < 0) {
    print "Can not open properties file for get SetupDir\n";
    $SetupDir = '/slview/nms/';
}

#2、DBType
my $DBType = GetSHDir('DBType');
if ($DBType < 0) {
    print "Can not open properties file for get DBType\n";
    $DBType = 'OraPub.pl';
}

unshift (@INC,"$SetupDir/lib");
require "$DBType";
require ("CommonPub.pl");
require ("Debug.pl");
require ("ZooKeeperPub.pl");

#进程判重
my $pid_dir = "$SetupDir/cfg/dat";
`mkdir -p $pid_dir` if( !-d $pid_dir);
my $g_pid_old = "";
if(-f "$pid_dir/UnzipDPIAccessLog.pid") {
    my $pid_old = `cat $pid_dir/UnzipDPIAccessLog.pid`;
    ($pid_old) = $pid_old =~ /(\d+)/;
    $g_pid_old = $pid_old;
    
    print "PIDFILE:$pid_old\n";
    my $pnum = 1;
    if(`uname` =~ /Linux/i) {
        $pnum = `ps uxww|grep $APPNAME|grep -v grep|grep -w $pid_old|wc -l`;
    }
    else{
        $pnum = `ps -ef|grep $APPNAME|grep -v grep|grep -w $pid_old|wc -l`;
    }
    $pnum =~ s/\D*//g;
    $pid_old = "" if($pnum == 0);
    
    if($pid_old > 0) {
        print "UnzipDPIAccessLog.pl is already running($pid_old),so skipped!\n";
        exit 2;
    }
}

my ($ProcNum, $DEBUG);
GetOptions( "p=s" => \$ProcNum,
            "d" => \$DEBUG);
if(!$DEBUG) {
    print "run as daemon\n";
    &Daemon();
}

#PID文件
print "进程号： $$\n";
if(open PID,">$pid_dir/UnzipDPIAccessLog.pid") {
    print PID $$;
    close PID;
}

############################ 全局变量 ############################
my $logDir = "/slview/nms/logs/PUB/";
`mkdir -p $logDir` if(!-d $logDir);
my $Import_Dir = "$SetupDir/data/import/accesslog/";
my $Deal_Dir = "$Import_Dir/running/";
`mkdir $Deal_Dir` if(!-d $Deal_Dir);
#my $Analysis_Dir = "$Import_Dir/Analysis";

my $lastday = &GetCurTime(6);
my $LogFileName = "$logDir/UnzipDPIAccessLog.$lastday.log";
open STDOUT, ">>$LogFileName" or die "ERROR: Redirecting STDOUT to $LogFileName: $!";
open STDERR, ">>$LogFileName" or die "ERROR: Redirecting STDERR to $LogFileName: $!";

#子进程最大个数
my $threadNum = $ProcNum > 0 ? $ProcNum : 5;

#存储处理或待处理文件名
my %g_PendFileHash = ();

#文件处理标记
my $DealFileFlag = 0;

print "$APPNAME start\n";

#收割子进程
our $curThread = 0;
$SIG{CHLD} = sub {
    my $child_pid;
    while (($child_pid = waitpid(-1, WNOHANG)) > 0) {
        print "##[$child_pid]## child $child_pid terminatedn,now have $curThread thread!\n";
        $curThread--;
    }
};

my $ZK_SERVER = &GetZKServerByZooCfg($ENV{ZOOKEEPER_HOME});
if($ENV{ZOOKEEPER_HOME} eq '') {
    print "please check env:ZOOKEEPER_HOME!\n";
}
elsif($ZK_SERVER eq ''){
    print "please check confure file:\$ZOOKEEPER_HOME/conf/zoo.cfg!\n";
}

while (1)
{
    &Load_hdfs_put_filelist_from_dir();

    &ProcessUnzipFile();
    
    #sleep 10;
    select(undef,undef,undef,0.05);
}

######################## Load_hdfs_put_filelist_from_dir() ##########################
#主要功能介绍、参数描述、返回值描述、编写/修改者、最后修改时间
#编写/修改者：chengfeng
#最后修改时间：20160301
#主要功能介绍: 装载文件
#参数描述：
#返回值描述：
###############################################################
sub Load_hdfs_put_filelist_from_dir(){
    my %FileType_gz = ();

    %g_PendFileHash = ();
    #将所有文件加载
    if(opendir INPUTDIR, $Import_Dir) {
        map {$FileType_gz{$_} = undef} grep /\.tar\.gz$/, readdir INPUTDIR;
        foreach my $file_name (%FileType_gz) {
            my ($file_prefix) = $file_name =~ /(\S+)\.tar\.gz/;
            next if(!-e "$Import_Dir/$file_prefix.ok");

            #合法的压缩文件装载移到running目录并将ok文件删除
            `mv $Import_Dir/$file_name $Deal_Dir`;
            unlink "$Import_Dir/$file_prefix.ok";
            $g_PendFileHash{$file_name} = undef;
    	}
        closedir INPUTDIR;
    }
    else {
        print "open dir $Import_Dir fail,$!\n";
        return -1;
    }
    
    my $curtime = &GetCurTime(7);
    my $LoadFileNum = keys %g_PendFileHash;
    print "[$curtime] Load File: $LoadFileNum\n";
}

######################## ProcessUnzipFile() ###################
#主要功能介绍、参数描述、返回值描述、编写/修改者、最后修改时间
#编写/修改者：chengfeng
#最后修改时间：20160301
#主要功能介绍: 创建多进程处理日志文件
#参数描述：
#返回值描述：
###############################################################
sub ProcessUnzipFile() {
    my %DealFileRun = %g_PendFileHash;
    my $file_count = 0;
    
    #在程序异常退出情况会将running目录文件重新装载
    if(($g_pid_old != $$) && (opendir RUNDIR, $Deal_Dir)) {
        $g_pid_old = $$;
        
        my @pendfile = grep /\.tar\.gz/, readdir RUNDIR;
        foreach (@pendfile) {
            $DealFileRun{$_} = undef;
    	}
        closedir RUNDIR;
    }
    #else {
    #    print "open running dir $Deal_Dir fail:$! || process num equal,can not load running dir file:$g_pid_old  $$\n";
    #}
    
    #print Dumper %DealFileRun;
    foreach my $file_name (keys %DealFileRun) {
        #处理文件垮天,日志日期变更
        my $curday = &GetCurTime(6);
        if ($lastday ne $curday) {
            $lastday = $curday;
            $DealFileFlag = 1;
            $LogFileName = "$logDir/UnzipDPIAccessLog.$lastday.log";
            open STDOUT, ">>$LogFileName" or die "ERROR: Redirecting STDOUT to $LogFileName: $!";
            open STDERR, ">>$LogFileName" or die "ERROR: Redirecting STDERR to $LogFileName: $!";
        }
        
        #并行个数已达到最大值 则等待
        while($curThread >= $threadNum) {
            sleep(1);
        }
        
        #print "**[$file_name]** ok_file:$file_prefix.ok txt_file:$file_prefix.txt\n";
        my $child_pid = fork();
        if (not defined $child_pid) { ##fork 失败
            print "##[$curThread]## resources not avilable when fork!\n";
            next;
        }
        ##子进程
        elsif ( $child_pid == 0) {
            $curThread++;
            print "##[$$]## CHILDPID=$$,the $curThread thread!\n";
            
            $SIG{CHLD} = 'DEFAULT';
            #my $log = `$unzip_cmd $file_name 2>&1`;
            #print "$unzip_cmd $file_name\n$log\n";
            &UnzipOneLog_put_2_hdfs($file_name);
            
            exit(0);
        }
        
        $file_count++;
        $curThread++;
    }
    
    my $curtime = &GetCurTime(7);
    print '@@['.$curtime.']@@'." FILECOUNT=$file_count\n";
    
    #已解压未分析数据仅保留1小时数据
    #`find $Analysis_Dir/ -mmin +60 -exec rm {} \\;`;
    
    #未解压分析数据 (每12小时处理一次)
    if ($DealFileFlag == 1) {
        $DealFileFlag == 0;
        `find $Import_Dir/*.gz -mtime +1 -exec rm {} \\;`;
        `find $Import_Dir/*.ok -mtime +1 -exec rm {} \\;`;
    }
}

######################## UnzipOneLog_put_2_hdfs() #######################
#主要功能介绍、参数描述、返回值描述、编写/修改者、最后修改时间
#编写/修改者：chengfeng
#最后修改时间：20160301
#主要功能介绍: 处理单个日志文件
#参数描述：
#返回值描述：
###############################################################
sub UnzipOneLog_put_2_hdfs() {
    my $file_name = shift;
    
    #解压
    `cd $Deal_Dir;gunzip -c $file_name |tar xvf -`;
    my ($file_prefix) = $file_name =~ /(\S+)\.tar\.gz/;
    #unlink "$Deal_Dir/$file_name";

    #获取机房ID
    my $first_line = `head -1 $Deal_Dir/$file_prefix.txt`;
    my ($houseid,$starttime_sec) = $first_line =~ /^([^\|]+).*\|(\d+)\s*$/;
    my $starttime = &GetCurTime(7,$starttime_sec);
    
    #获取文件时间，到整5分钟
    my $start_min;
    my ($start_year,$start_day,$start_tenMin,$start_smin) = $starttime =~ /^(\d{4})(\d{4})(\d{3})(\d)/;
    if($start_smin >= 5) {
        $start_min = $start_tenMin."5";
    }
    else{
        $start_min = $start_tenMin."0";
    }
    
    my %put_files;
    #55分钟数据需要检查是否跨小时
    if($start_min eq '55') {
        my $end_sec = &ParseTimeString($start_year.$start_day,$start_min)+300;
        my $end_time = &GetCurTime(7,$end_sec);
        my ($end_year,$end_day,$end_min) = $end_time =~ /^(\d{4})(\d{4})(\d{4})/;
        my $last_line = `tail -1 $Deal_Dir/$file_name`;
        my ($last_time) = $last_line =~ /\|(\d+)\s*$/;
        
        #需切割文件
        if($last_time >= $end_sec) {
            #打开文件
            unless(open ACCESSLOG,"$Deal_Dir/$file_prefix.txt") {
                print "open $Deal_Dir/$file_prefix.txt error:$!\n";
                exit(4);
            }
            
            unless(open ACCESSLOG_NEW1,">$Deal_Dir/$file_prefix.1.txt") {
                print "open $Deal_Dir/$file_prefix.2.txt error:$!\n";
                exit(5);
            }
            
            unless(open ACCESSLOG_NEW2,">$Deal_Dir/$file_prefix.2.txt") {
                print "open $Deal_Dir/$file_prefix.2.txt error:$!\n";
                exit(5);
            }
            
            my ($line,$time_sec);
            while($line = <ACCESSLOG>) {
                my ($time_sec) = $line =~ /\|(\d+)\s*$/;
                
                if($time_sec < $end_sec){
                    print ACCESSLOG_NEW1 $line;
                }
                else{
                    print ACCESSLOG_NEW2 $line;
                }
            }
            close ACCESSLOG;
            close ACCESSLOG_NEW1;
            close ACCESSLOG_NEW2;
            
            $put_files{"$Deal_Dir/$file_prefix.1.txt"} = "/hadoop/accesslog/$start_year/$start_day/$start_min/$houseid/$file_prefix.txt";
            $put_files{"$Deal_Dir/$file_prefix.2.txt"} = "/hadoop/accesslog/$end_year/$end_day/$end_min/$houseid/$file_prefix.txt";
            unlink "$Deal_Dir/$file_prefix.txt";
        }
        else{
            $put_files{"$Deal_Dir/$file_prefix.txt"} = "/hadoop/accesslog/$start_year/$start_day/$start_min/$houseid/$file_prefix.txt";
        }
    }
    else{
        $put_files{"$Deal_Dir/$file_prefix.txt"} = "/hadoop/accesslog/$start_year/$start_day/$start_min/$houseid/$file_prefix.txt";
    }
    
    #上传至hadf
    foreach my $local_file(keys %put_files) {
        my $remote_file = $put_files{$local_file};
        my ($hdfs_dir) = $remote_file =~ /^(.*)\//;
        
        #创建目录
        my $cmd = "\$HADOOP_HOME/bin/hadoop dfs -mkdir -p $hdfs_dir";
        print "$cmd\n";
        `$cmd`;
        
        #上传文件
        $cmd = "\$HADOOP_HOME/bin/hadoop dfs -moveFromLocal $local_file $remote_file";
        print "$cmd\n";
        `$cmd`;
        
        #入到zookeeper中
        $cmd = "echo 'create -s /hdfsqueue/accesslog $remote_file' | zkCli.sh -server $ZK_SERVER";
        print "$cmd\n";
        `$cmd`;
    }
    
    `rm -f $Deal_Dir/$file_prefix.*`;
    #unlink "$Deal_Dir/$file_prefix.*";
}

sub Daemon() {
    my $child = fork();
    unless(defined $child){
        print "can't fork,so exit!\n";
    }

    exit 0 if $child;
    setsid();
    open (STDIN, "</dev/null");
    open (STDOUT, ">/dev/null");
    #open (STDERR,">&STDOUT");
    chdir $SetupDir;
    umask(022);
    #$ENV{PATH}='/bin:/usr/bin:/sbin:/usr/sbin';
    return $$;
}

######################## GetCurTime() #############################
#主要功能介绍、参数描述、返回值描述、编写/修改者、最后修改时间
#编写/修改者：chengfeng
#最后修改时间：20150229
#主要功能介绍:获取系统时间
#参数描述：
#返回值描述： 
#############################################################
sub GetCurTime() {
    my ($type, $cursec) = @_;

    $cursec = time if(!defined $cursec || $cursec eq '');
    my ($sec, $min, $hour, $mday, $mon, $year, $wday, $yday, $isdst) = localtime($cursec);
    $mon = $mon+1;
    $mday = $mday;
    $year += 1900;

    # formate
    if($type =~ /\%/){
        return sprintf($type, $year, $mon, $mday, $hour,$min,$sec);
    }
    # formate yyyy/mm/dd/ hh24/mi/ss
    elsif($type =~ /yyyy|mm|dd|hh24|mi|ss/i){
        $year = sprintf("%04d",$year);
        $mon = sprintf("%02d",$mon);
        $mday = sprintf("%02d",$mday);
        $hour = sprintf("%02d",$hour);
        $min = sprintf("%02d",$min);
        $sec = sprintf("%02d",$sec);

        $type =~ s/yyyy/$year/i;
        $type =~ s/mm/$mon/i;
        $type =~ s/dd/$mday/i;
        $type =~ s/hh24/$hour/i;
        $type =~ s/hh/$hour/i;
        $type =~ s/mi/$min/i;
        $type =~ s/ss/$sec/i;
        return $type;
    }
    # yyyy
    elsif($type == 1){
        return sprintf("%04d", $year);
    }
    # yyyymm
    elsif($type == 2){
        return sprintf("%04d%02d", $year, $mon);
    }
    # yyyy-mm-dd
    elsif($type == 3){
        return sprintf("%04d-%02d-%02d", $year, $mon, $mday);
    }
    # yyyymmddhh24
    elsif($type == 4){
        return sprintf("%04d%02d%02d%02d", $year, $mon, $mday, $hour);
    }
    # yyyymmddhh24mi
    elsif($type == 5){
        return sprintf("%04d%02d%02d%02d%02d", $year, $mon, $mday, $hour, $min);
    }
    elsif($type == 6){
        return sprintf("%04d%02d%02d", $year, $mon, $mday);
    }
    # yyyymmddhh24miss
    else{
        return sprintf("%04d%02d%02d%02d%02d%02d", $year, $mon, $mday, $hour, $min, $sec);
    }
}
