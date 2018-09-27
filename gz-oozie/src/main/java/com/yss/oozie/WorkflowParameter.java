package com.yss.oozie;

import java.util.Objects;

/**
 * @author : 张海绥
 * @version : 2018-8-8
 *  describe: 用于封装jobproperties中的参数
 *  目标文件：09000211trddata20180420.txt
 *  目标表：QHCJMX
 */
public class WorkflowParameter {
    private String name;
    private  String value;

    public WorkflowParameter(String name, String value) {
        this.name = name;
        this.value = value;
    }

    public WorkflowParameter() {
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WorkflowParameter that = (WorkflowParameter) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {

        return Objects.hash(name, value);
    }

    @Override
    public String toString() {
        return "WorkflowParameter{" +
                "name='" + name + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
