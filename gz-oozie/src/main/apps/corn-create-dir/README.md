
# 原数据每天24:00生成当天的目录，
# 本地目录/data/gz_interface/日期
# 机器：192.168.102.114 



# 1.创建&清理目录

hadoop fs -mkdir -p /tmp/zl/apps

hadoop fs -rm -r /tmp/zl/apps/corn-create-dir

# 2.上传文件

hadoop fs -put /data/temp/zl/ooize/corn-create-dir /tmp/zl/apps 


# 3.执行&停止

oozie job -oozie http://bj-rack001-hadoop004:11000/oozie -config /data/temp/zl/ooize/corn-create-dir/job.properties -run

oozie job -oozie http://bj-rack001-hadoop004:11000/oozie -kill 0000002-180926140052456-oozie-oozi-C



# 4.验证












