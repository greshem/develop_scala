dir -1 /slview/nms/data/uddata/01e0/dbinfo/*.db > /tmp/tmp
split -l 1000  /tmp/tmp  prefix_linux_100

for each in $(dir -1 |grep prefix_linux)
do
touch $(cat  $each)
python product_lines.py 
done
