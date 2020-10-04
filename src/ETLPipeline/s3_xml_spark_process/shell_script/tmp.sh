spark-submit \
--master spark://172.31.49.77:7077 \
--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11 \
--packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7 \
--conf spark.executor.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true \
--conf spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true  \
--jars spark-xml_2.11-0.6.0.jar \
--driver-class-path /usr/local/spark/jars/postgresql-42.2.16.jar   --jars /usr/local/spark/jars/postgresql-42.2.16.jar \
--total-executor-cores 13 \
--executor-cores 5 \
--executor-memory 10G \
--driver-memory 2g \
tmp.py
