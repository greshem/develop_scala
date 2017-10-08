##############################################################################
use strict;
use DBI;
use Getopt::Long;
use GetSHDir;
use WriteErrLog;
use POSIX qw(strftime);
use Time::HiRes qw(time);
use Time::Local;
use Data::Dumper;
use Encode;
#--test
use lib "/slview/nms/lib/","/slview/nms/pm/";
use UD_CODE;
my ($statement,$sth,$dbh,$ref);#Êý¾Ý¿â±äÁ¿
my $APPNAME="DPI-UdFileImport.pl";
##############################################
##############################################
my $SetupDir = GetSHDir("SetupDir");
my $DBtype = GetSHDir("DBType");
my $oradb=GetSHDir('DBString');
exit (-1) if($oradb < 0);
my $orauser=GetSHDir('DBUserName');
exit (-1) if($orauser < 0);
my $orapsw=GetSHDir('DBPasswd');
exit (-1) if($orapsw < 0);
my $ud2Type=GetSHDir('UD2TYPE');
$ud2Type = 1 if($ud2Type < 0);
my $ud2filenum = GetSHDir('ud2filenum');
$ud2filenum = 1 if($ud2filenum < 0);
#--test
#$oradb = '91DBNMS_DPI';
#$orauser = 'dpitest';
#$orapsw = 'dpitest';
#»ñµÃËùÐèº¯Êý
unshift (@INC,"$SetupDir/lib");
require ("$DBtype");
require ("Debug.pl");
require ("CommonPub.pl");




my $dbh = &dbCon("$DBtype"); 
my $statement = "select dpidevcode,dpidevaddr from dpidevice where changetype=0";
my $sth = &doselect($dbh,$statement);
my $ref;
my %DPIMAP = qw();
while($ref=$sth->fetchrow_arrayref()){
	print  $ref->[0]."\n";;
   #$DPIMAP{$ref->[1]} = $ref->[0];
}
