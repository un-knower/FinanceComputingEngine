# 多个workflow 串联



# 1.创建&清理目录

hadoop fs -mkdir -p /tmp/zl/apps

hadoop fs -rm -r /tmp/zl/apps/sub-workflow


# 2.上传文件

hadoop fs -put /data/temp/zl/ooize/sub-workflow /tmp/zl/apps 


# 3.执行&停止

oozie job -oozie http://bj-rack001-hadoop004:11000/oozie -config /data/temp/zl/ooize/sub-workflow/main-workflow/job.properties -run

oozie job -oozie http://bj-rack001-hadoop004:11000/oozie -kill 0000649-180913100125560-oozie-oozi-C


# 4.验证












