package com.yss.oozie;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.WorkflowJob.Status;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

public class WorkFlowClient {
    private static String OOZIE_URL = "http://192.168.102.122:11000/oozie/";//oozie提交运行时用到的命令
    private static String JOB_PATH = "hdfs://bj-rack001-hadoop002/tmp/examples/apps/spark/workflow.xml";//workflow的路径
    private static String JOB_Tracker = "bj-rack001-hadoop003:8050";
    private static String NAMENode = "hdfs://bj-rack001-hadoop002:8020";

    OozieClient wc = null;

    public WorkFlowClient(String url){
        wc = new OozieClient(url);
    }

    public String startJob(String wfDefinition, List<WorkflowParameter> wfParameters)
            throws OozieClientException {

        // create a workflow job configuration and set the workflow application path
        Properties conf = wc.createConfiguration();
        conf.setProperty(OozieClient.APP_PATH, wfDefinition);

        // setting workflow parameters
        conf.setProperty("jobTracker", JOB_Tracker);
        conf.setProperty("nameNode", NAMENode);
        if((wfParameters != null) && (wfParameters.size() > 0)){
            for(WorkflowParameter parameter : wfParameters)
                conf.setProperty(parameter.getName(), parameter.getValue());
        }
        // submit and start the workflow job
        return wc.run(conf);
    }

    public Status getJobStatus(String jobID) throws OozieClientException{
        WorkflowJob job = wc.getJobInfo(jobID);
        return job.getStatus();
    }

    public static void main(String[] args) throws OozieClientException, InterruptedException{

        // Create client
        WorkFlowClient client = new WorkFlowClient(OOZIE_URL);
        // Create parameters
        List<WorkflowParameter> wfParameters = new LinkedList<WorkflowParameter>();
        WorkflowParameter queueName = new WorkflowParameter("queueName","default");
        WorkflowParameter username = new WorkflowParameter("user.name","hadoop");
        WorkflowParameter master = new WorkflowParameter("master","local[*]");
        //WorkflowParameter jobmode = new WorkflowParameter("jobmode","client");
       // WorkflowParameter jobname = new WorkflowParameter("jobname","QiHuoChengJiaoMingXi");
        //WorkflowParameter jarclass = new WorkflowParameter("jarclass","com.yss.scala.guzhi.QiHuoChengJiaoMingXi");
        WorkflowParameter examplesRoot = new WorkflowParameter("examplesRoot","examples");
        WorkflowParameter syspath = new WorkflowParameter("oozie.use.system.libpath","true");
        WorkflowParameter ooziepath = new WorkflowParameter("oozie.libpath", "hdfs://bj-rack001-hadoop002/user/oozie/share/lib/spark");
        //WorkflowParameter flag = new WorkflowParameter("flag","1");
        //WorkflowParameter jarpath = new WorkflowParameter("jarpath","hdfs://bj-rack001-hadoop002:8020/tmp/examples/oozie-examples-4.2.0.jar");
 //       WorkflowParameter sparkopts = new WorkflowParameter("sparkopts","--num-executors 3 --executor-memory 1G --executor-cores 2 --driver-memory 2G " +
//                " --conf spark.yarn.jar=hdfs://bj-rack001-hadoop002:8020/bobo/in/flow/spark-assembly-1.6.0-cdh5.9.0-hadoop2.6.0-cdh5.9.0.jar");
//        WorkflowParameter jararg1 = new WorkflowParameter("jararg1","slave01:9092,slave02:9092,slave03:9092");
//        WorkflowParameter jararg2 = new WorkflowParameter("jararg2","DATA-TOPIC");
        wfParameters.add(queueName);
        wfParameters.add(username);
        wfParameters.add(master);
        wfParameters.add(examplesRoot);
        wfParameters.add(syspath);
        wfParameters.add(ooziepath);
        //wfParameters.add(flag);
        //wfParameters.add(jobmode);
        //wfParameters.add(jobname);
        //wfParameters.add(jarclass);
        //wfParameters.add(jarpath);
//        wfParameters.add(sparkopts);
//        wfParameters.add(jararg1);
//        wfParameters.add(jararg2);

        // Start Oozing
        String jobId = client.startJob(JOB_PATH, wfParameters);
        Status status = client.getJobStatus(jobId);
        if(status == Status.RUNNING)
            System.out.println("Workflow job running");
            //System.out.println(wc.getJobId(jobId))
        else
            System.out.println("Problem starting Workflow job");
    }
}