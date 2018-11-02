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

	 nohup flume-ng agent -c conf -f gz_tailfile.conf --name a1 &

	
	
5.开发中对集群Flume下添加的jar文件
        5.1
		 <!--解析DBF文件-->
                <dependency>
                    <groupId>com.github.albfernandez</groupId>
                    <artifactId>javadbf</artifactId>
                    <version>1.9.2</version>
                </dependency>
                <!--解析Xml文件-->
                <dependency>
                    <groupId>org.json</groupId>
                    <artifactId>json</artifactId>
                    <version>20180813</version>
                </dependency>
                <!--解析xls文件-->
                <dependency>
                    <groupId>org.apache.poi</groupId>
                    <artifactId>poi</artifactId>
                    <version>4.0.0</version>
                </dependency>
                <!--解析xlsx文件-->
                <dependency>
                    <groupId>org.apache.poi</groupId>
                    <artifactId>poi-ooxml</artifactId>
                    <version>4.0.0</version>
                </dependency>
                <dependency>
                    <groupId>org.apache.commons</groupId>
                    <artifactId>commons-compress</artifactId>
                    <version>1.18</version>
                </dependency>
                <dependency>
                    <groupId>org.apache.poi</groupId>
                    <artifactId>poi-ooxml-schemas</artifactId>
                    <version>4.0.0</version>
                </dependency>
                <dependency>
                    <groupId>org.apache.xmlbeans</groupId>
                    <artifactId>xmlbeans</artifactId>
                    <version>3.0.1</version>
                </dependency>
                
      5.2        
       114机器Flume的lib下的commons-compress-1.4.1 替换了新版本 commons-compress-1.18.jar,在读xlsx文件时找不到对应的类

6.自定义的FlumeSource

     6.1 spooldir 
     
       com.yss.source.spooldir.SpoolDirectorySource
       
     6.2 taildir
     
       com.yss.source.taildir.TaildirSource
      
7.自定义的FlumeSink

       com.yss.sink.hdfs.HDFSEventSink
      
8.需求

    1、自定义source和sink
    2、在source端解析dbf和xml以及tsv,txt,xls,xlxs后缀的文件转换成csv中间分割符为\t
    3、sink保留原文件名字后缀名为.csv
    #4、原文件传输完成之后，文件名加一个时间戳以及.xlsd后缀(目前没有修改,功能已经实现)
    5、传到hdfs上的文件中间状态加个.TMP后缀
    6、支持监控多级目录的递归,各级目录都可以监控

	
