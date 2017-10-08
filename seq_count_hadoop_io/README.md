#参考  ../spark-example-project/
#==========================================================================
#https://github.com/soulmachine/spark-example-project


#spark-submit --class me.soulmachine.spark.WordCount --master yarn://ip-or-host:7077 ./spark-example-project-1.0.jar wordcount-test/input wordcount-test/outputs

seq_file=$1

if [ $# == 0 ];then
echo "Usage:  seq_file";
exit -1 
fi

if [ ! -f  $seq_file ];
then
        echo  " $seq_file  input is not file ";
        exit -1;
fi

rm -rf /tmp/file_list/
spark-submit --class me.soulmachine.spark.WordCount --master local[4]  target/scala-2.10/seq_count_2.10-1.0.0-SNAPSHOT.jar   $seq_file
