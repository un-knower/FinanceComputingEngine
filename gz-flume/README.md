#****************************************************************************************
#*** 说明文档: 自定义Flume组件总体架构阐述!
#*** 编 写 人:  wangshuai
#*** 编写日期:  2018-09-21
#*** 修 改 人:
#*** 修改日期:
#*** 备    注:
#*****************************************************************************************


1.Flume监控Linux上的路径

		目录	/data/gz_interface/ 	
		
		机器 192.168.102.114
		
2.Flume上传文件到hdfs的路径为

		/yss/guzhi/interface/

3.Flume运行脚本
		
    见gz-flume/resources/目录下的脚本文件 


4.Flume后台运行指令

	nohup	flume-ng agent -c conf -f testsource_1.conf --name a1
	
	
5.开发中对集群Flume下添加的jar文件

		<dependency>
           <groupId>com.github.albfernandez</groupId>
            <artifactId>javadbf</artifactId>
           <version>1.9.2</version>
        </dependency>
		
        <dependency>
            <groupId>org.json</groupId>
            <artifactId>json</artifactId>
            <version>20180813</version>
        </dependency>

6.自定义的FlumeSource

     6.1 spooldir 
     
       com.yss.source.spooldir.SpoolDirectorySource
       
     6.2 taildir
     
       com.yss.source.taildir.TaildirSource
      
7.自定义的FlumeSink

       com.yss.sink.hdfs.HDFSEventSink
      
8.需求

    1、自定义source和sink
    2、在source端解析dbf和xml以及tsv后缀的文件转换成csv中间分割符为逗号
    3、sink保留原文件名字后缀名为.csv
    4、原文件传输完成之后，文件名加一个时间戳以及.xlsd后缀
    5、传到hdfs上的文件中间状态加个.TMP后缀
    6、支持监控多级目录的递归,各级目录都可以监控

	
