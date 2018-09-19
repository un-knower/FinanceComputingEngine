package com.yss.oozie;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;

import java.util.Properties;


/**
 * @author zhanghaisui
 * @version 2018-09-14 11:21
 * describe:
 * 目标文件：
 * 目标表：
 */
public class WorkFlowClientYarn {
    public static void main(String[] args) {
        //get a OozieClient for local Oozie
        OozieClient wc = new OozieClient("http://192.168.102.122:11000/oozie");

        //create workflow job configuration
        Properties conf = wc.createConfiguration();
        //这可以换成cordinater和bundle，只能是其中一个
       // conf.setProperty(OozieClient.APP_PATH, "hdfs://bj-rack001-hadoop002/tmp/zhs/localOozie/workflow.xml");
        conf.setProperty(OozieClient.APP_PATH, "hdfs://bj-rack001-hadoop002/tmp/zhs/yarnOozie/workflow.xml");

        //set a workflow parameters
        conf.setProperty("jobTracker", "bj-rack001-hadoop003:8050");
        conf.setProperty("nameNode", "hdfs://bj-rack001-hadoop002:8020");
        conf.setProperty("master", "yarn-client");
        conf.setProperty("jobmode", "client");
        conf.setProperty("jobname", "QiHuoChengJiaoMingXi");
        conf.setProperty("jarclass", "com.yss.scala.guzhi.QiHuoChengJiaoMingXi");
        conf.setProperty("jarpath", "hdfs://bj-rack001-hadoop002:8020/tmp/zhs/localOozie/gz-business-1.0.0-jar-with-dependencies.jar");
        conf.setProperty("queueName", "default");
        conf.setProperty("examplesRoot", "examples");
        conf.setProperty("user.name", "hadoop");
        conf.setProperty("oozie.use.system.libpath", "true");
        conf.setProperty("oozie.libpath", "hdfs://bj-rack001-hadoop002/user/oozie/share/lib/spark2");

        //submit and start the workflow job
        try{
            String jobId = wc.run(conf);
            System.out.println("Workflow job submitted");

            //wait until the workflow job finishes
            while(wc.getJobInfo(jobId).getStatus() == WorkflowJob.Status.RUNNING){
                System.out.println("Workflow job running...");
                try{
                    Thread.sleep(10*1000);
                }catch(InterruptedException e){e.printStackTrace();}
            }
            System.out.println("Workflow job completed!");
            System.out.println(wc.getJobId(jobId));
        }catch(OozieClientException e){e.printStackTrace();}

    }
}
