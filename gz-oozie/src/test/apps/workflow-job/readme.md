
hadoop fs -rm -r /tmp/zl/apps/workflow-job
hadoop fs -put /data/temp/zl/ooize/workflow-job /tmp/zl/apps 



corn-create-dir


oozie job -oozie http://bj-rack001-hadoop004:11000/oozie -config /data/temp/zl/ooize/workflow-job/corn-create-dir.properties -run

========================================================

job-corn-production-data

oozie job -oozie http://bj-rack001-hadoop004:11000/oozie -config /data/temp/zl/ooize/workflow-job/job-corn-production-data.properties -run


========================================================

job-corn-del-data

oozie job -oozie http://bj-rack001-hadoop004:11000/oozie -config /data/temp/zl/ooize/workflow-job/job-corn-del-data.properties -run











oozie job -oozie http://bj-rack001-hadoop004:11000/oozie -kill 0000016-180926140052456-oozie-oozi-C












