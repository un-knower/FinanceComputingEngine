package com.yss.oozie;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;

import java.util.Properties;

/**
 * @author zhanghaisui
 * @version 2018-10-18 13:43
 * describe:调度估值项目的API，通过java的API直接调度执行，不需要命令行提交
 * 目标文件：
 * 目标表：
 */
public class GuZhiOozie {
    public static void main(String[] args) {
        //创建客户端oozie，指定oozieServer
        OozieClient wc = new OozieClient("http://192.168.102.122:11000/oozie");

        Properties conf = wc.createConfiguration();
        //指定workflow路径
        conf.setProperty(OozieClient.APP_PATH, "hdfs://bj-rack001-hadoop002/tmp/zhs/spark-sqoop/workflow.xml");

        //设置workflow所需要的参数
        conf.setProperty("jobTracker", "bj-rack001-hadoop003:8050");
        conf.setProperty("nameNode", "hdfs://bj-rack001-hadoop002:8020");
        conf.setProperty("master", "yarn-cluster");
        conf.setProperty("jobmode", "cluster");
        conf.setProperty("jobname1", "Ggt");
        conf.setProperty("jobname2", "ShFICCTriPartyRepoETL");
        conf.setProperty("jobname3", "SHTransfer");
        conf.setProperty("jobname4", "SZSETriPartyRepo");
        conf.setProperty("jobname5", "SZStockExchange");
        conf.setProperty("jarclass1", "com.yss.scala.guzhi.Ggt");
        conf.setProperty("jarclass2", "com.yss.scala.guzhi.ShFICCTriPartyRepoETL");
        conf.setProperty("jarclass3", "com.yss.scala.guzhi.SHTransfer");
        conf.setProperty("jarclass4", "com.yss.scala.guzhi.SZSETriPartyRepo");
        conf.setProperty("jarclass5", "com.yss.scala.guzhi.SZStockExchange");
        conf.setProperty("shell", "getDate.sh");
        conf.setProperty("jarpath", "hdfs://bj-rack001-hadoop002:8020/tmp/zhs/spark-sqoop/gz-business-1.0.0-jar-with-dependencies.jar");
        conf.setProperty("queueName1", "launcher");
        conf.setProperty("queueName", "default");
        conf.setProperty("queueName2", "AQ");
        conf.setProperty("examplesRoot", "zhs");
        conf.setProperty("user.name", "hadoop");
        conf.setProperty("oozie.use.system.libpath", "true");
        conf.setProperty("oozie.libpath", "hdfs://bj-rack001-hadoop002:8020/user/oozie/share/lib/sqoop,hdfs://bj-rack001-hadoop002:8020/user/oozie/share/lib/spark2");
        conf.setProperty("arg1", "hdfs://bj-rack001-hadoop002:8020/yss/guzhi/interface/20181016/shsfhg-gdsy/jsmx03_jsjc1.*.csv");
        conf.setProperty("arg2", "hdfs://bj-rack001-hadoop002:8020/yss/guzhi/interface/20181016/shsfhg-gdsy/wdqjsjc1.*.csv");
        conf.setProperty("arg3", "20181016/gh1014.csv");
        conf.setProperty("arg4", "SHTransfer");
        conf.setProperty("security_enabled", "False");
        conf.setProperty("dryrun", "False");
        conf.setProperty("sparkopts", "--num-executors 2 --executor-memory 2g --executor-cores 2 --driver-memory 2g");

        //提交和运行workflow
        try{
            String jobId = wc.run(conf);
            System.out.println("Workflow job submitted");
            System.out.println(jobId);
            while(true){
                //获取oozie运行状态
                WorkflowJob.Status status= wc.getJobInfo(jobId).getStatus();
                if (status == WorkflowJob.Status.RUNNING) {
                    System.out.println("Workflow job running...");
                } else if (status == WorkflowJob.Status.SUCCEEDED) {
                    System.out.println("Workflow job success");
                    break;
                } else {
                    System.out.println("Workflow job failed");
                    break;
                }

                try{
                    Thread.sleep(10*1000);
                }catch(InterruptedException e){
                    e.printStackTrace();
                }
            }
        }catch(OozieClientException e){
            e.printStackTrace();
        }

    }
}
