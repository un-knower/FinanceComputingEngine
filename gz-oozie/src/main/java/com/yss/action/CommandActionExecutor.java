package com.yss.action;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import org.apache.commons.io.IOUtils;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.Namespace;

import java.io.IOException;
import java.io.InputStream;

/**
 * author:   ZhangLong
 * since:     2018/9/17
 * describe : 自定义登录到其他机器，执行shell命令
 *
 *  host 机器地址
 *  port 端口
 *  user 登录用户
 *  password   登录密码
 *  command 执行命令
 *
 *  1.需要打成jar文件 :
 *      mvn clean package -Dmaven.test.skip=true
 *  2.上传文件到 oozie-server 所在的服务器oozie安装目录（bj-rack001-hadoop004 ）
 *      rm -fr /usr/hdp/current/oozie-server/libext/gz-oozie-1.0.0.jar
 *      scp gz-oozie-1.0.0.jar root@bj-rack001-hadoop004:/usr/hdp/current/oozie-server/libext
 *  3.更改oozie-site.xml配置文件 调整参数&重启服务
 *      oozie.service.ActionService.executor.ext.classes=com.yss.action.CommandActionExecutor
 *      oozie.service.SchemaService.wf.ext.schemas=command-action.xsd
 *
 *  4.编写workflow文件，测试使用.......
 */



public class CommandActionExecutor extends org.apache.oozie.action.ActionExecutor {

    public static final String ACTION_TYPE = "command";

    private static final String SUCCEEDED = "OK";
    private static final String FAILED = "FAIL";
    private static final String KILLED = "KILLED";

    public CommandActionExecutor() {
        super(ACTION_TYPE);
    }

    @Override
    public void start(Context context, WorkflowAction action) throws ActionExecutorException {
        // Get parameters from Node configuration
        try {
            Element actionXml = XmlUtils.parseXml(action.getConf());
            Namespace ns = Namespace
                    .getNamespace("uri:custom:command-action:0.1");

            System.out.println("============== command  start =======================");

            String host = actionXml.getChildTextTrim("host", ns);
            int port = Integer.valueOf(actionXml.getChildTextTrim("port", ns));
            String user = actionXml.getChildTextTrim("user", ns);
            String password = actionXml.getChildTextTrim("password", ns);
            String command = actionXml.getChildTextTrim("command", ns);

            exeCommand(host,  port,  user,  password,  command);

            context.setExecutionData(SUCCEEDED, null);
            System.out.println("============== command end  =======================");
        } catch (Exception e) {
            context.setExecutionData(FAILED, null);
            throw new ActionExecutorException(ActionExecutorException.ErrorType.FAILED,
                    ErrorCode.E0000.toString(), e.getMessage());
        }
    }

    @Override
    public void end(Context context, WorkflowAction action) throws ActionExecutorException {
        String externalStatus = action.getExternalStatus();
        WorkflowAction.Status status = externalStatus.equals(SUCCEEDED) ? WorkflowAction.Status.OK
                : WorkflowAction.Status.ERROR;
        context.setEndData(status, getActionSignal(status));
    }

    @Override
    public void check(Context context, WorkflowAction workflowAction) throws ActionExecutorException {

    }

    @Override
    public void kill(Context context, WorkflowAction workflowAction) throws ActionExecutorException {
        context.setExternalStatus(KILLED);
        context.setExecutionData(KILLED, null);
    }

    @Override
    public boolean isCompleted(String s) {
        return true;
    }


    /**
     *  执行shell命令程序
     * @param host
     * @param port
     * @param user
     * @param password
     * @param command
     * @return
     * @throws JSchException
     * @throws IOException
     */
    public String exeCommand(String host, int port, String user, String password, String command) throws JSchException, IOException {

        JSch jsch = new JSch();
        Session session = jsch.getSession(user, host, port);
        session.setConfig("StrictHostKeyChecking", "no");

        session.setPassword(password);
        session.connect();

        ChannelExec channelExec = (ChannelExec) session.openChannel("exec");
        InputStream in = channelExec.getInputStream();
        channelExec.setCommand(command);
        channelExec.setErrStream(System.err);
        channelExec.connect();
        String out = IOUtils.toString(in, "UTF-8");
        channelExec.disconnect();
        session.disconnect();

        return out;
    }


}

