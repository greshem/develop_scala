

#2017_07_16_23:34:51   星期日   add by greshem

#所有的commanids 
python  product_lines.py  --input_path  /slview/nms/data/uddata/test/    --once 1 --delete_file 0 --array_count 15  --split_str ','  

#一个command_id 
python  product_lines.py  --input_path  /slview/nms/data/uddata/test/    --once 1 --delete_file 0 --array_count 15  --split_str ','  --commandid  479 


#运行一次, 分隔符 切换到 线上环境"<<!>>>",  一行有16个元素 .
 python  product_lines.py  --input_path  /root/data/dbinfo0506/    --once 1 --delete_file 0 --array_count 16  --split_str "<<<!>>>"
/slview/nms/data/uddata/test/delete/
/slview/nms/data/uddata/test/DPIIDCISMLOG.1500465676.39189.db

#用于 微秒 的输出
 python  product_lines.py  --input_path  /root/data/dbinfo0506//delete//delete/delete/delete/   --once 1 --delete_file 0  --mirco_second  1

#文件不进行删除 保留 便于测试. 只 运行一次.
 python  product_lines.py  --input_path  /root/data/dbinfo0506//delete//delete/delete/delete/   --once 1 --delete_file 0

#只运行一次 打印统计信息 然后退出 
python product_lines.py  --input_path=/root/data/dbinfo0506/    --once 1

#loop 运行
python product_lines.py  --input_path=/root/data/dbinfo0506/    --once 0
