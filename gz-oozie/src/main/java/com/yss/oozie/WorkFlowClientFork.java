package com.yss.oozie;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;

import java.util.Properties;

/**
 * @author zhanghaisui
 * @version 2018-09-14 15:01
 * describe:JavaAPI调用oozie任务，yarn模式执行oozie中的fork，方法二
 * 目标文件：
 * 目标表：
 */
public class WorkFlowClientFork {
    public static void main(String[] args) {
        //创建客户端oozie，指定oozieServer
        OozieClient wc = new OozieClient("http://192.168.102.122:11000/oozie");

        Properties conf = wc.createConfiguration();
        //指定workflow路径
        conf.setProperty(OozieClient.APP_PATH, "hdfs://bj-rack001-hadoop002/tmp/zhs/forkOozie/workflow.xml");

        //设置workflow所需要的参数
        conf.setProperty("jobTracker", "bj-rack001-hadoop003:8050");
        conf.setProperty("nameNode", "hdfs://bj-rack001-hadoop002:8020");
        conf.setProperty("master", "yarn-client");
        conf.setProperty("jobmode", "client");
        conf.setProperty("jobname", "QiHuoChengJiaoMingXi");
        conf.setProperty("jobname2", "forkTest");
        conf.setProperty("jarclass", "com.yss.scala.guzhi.QiHuoChengJiaoMingXi");
        conf.setProperty("jarclass2", "com.yss.scala.guzhi.forkTest");
        conf.setProperty("jarpath", "hdfs://bj-rack001-hadoop002:8020/tmp/zhs/forkOozie/gz-business-1.0.0-jar-with-dependencies.jar");
        conf.setProperty("queueName", "default");
        conf.setProperty("examplesRoot", "examples");
        conf.setProperty("user.name", "hadoop");
        conf.setProperty("oozie.use.system.libpath", "true");
        conf.setProperty("oozie.libpath", "hdfs://bj-rack001-hadoop002/user/oozie/share/lib/spark2");

        //提交和运行workflow
        try{
            String jobId = wc.run(conf);
            System.out.println("Workflow job submitted");
            System.out.println(jobId);

            while(true){
                if (wc.getJobInfo(jobId).getStatus() == WorkflowJob.Status.RUNNING) {
                    System.out.println("Workflow job running...");
                } else if (wc.getJobInfo(jobId).getStatus() == WorkflowJob.Status.SUCCEEDED) {
                    System.out.println("Workflow job success");
                    break;
                } else {
                    System.out.println("Workflow job failed");
                    break;
                }

                try{
                    Thread.sleep(10*1000);
                }catch(InterruptedException e){e.printStackTrace();}
            }
        }catch(OozieClientException e){e.printStackTrace();}

    }
}
