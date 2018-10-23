1.主workflow.xml在HDFS上的文件路径

		目录	/tmp/zhs/spark-sqoop/workflow.xml
		
		
2.两个子workflow.xml在HDFS上的文件路径

		目录sqoop：/tmp/zhs/spark-sqoop/sqoop/workflow.xml
		目录spark：/tmp/zhs/spark-sqoop/spark/workflow.xml

3.程序jar包在HDFS上的文件路径
		
    目录	/tmp/zhs/spark-sqoop/gz-business-1.0.0-jar-with-dependencies.jar


4.集群运行oozie任务的指令

	机器 192.168.102.114
	oozie job -oozie http://192.168.102.122:11000/oozie -config /data/temp/zhs/spark-sqoop/job.properties -run
	
	
