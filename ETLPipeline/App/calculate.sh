beginYear=$1
endYear=$2
filterNumber=$3
echo $beginYear
echo $endYear
echo $filterNumber
spark-submit \
--master spark://172.31.49.77:7077 \
--packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7 \
--conf spark.executor.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true \
--conf spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true  \
--jars spark-xml_2.11-0.6.0.jar \
--driver-class-path /usr/local/spark/jars/postgresql-42.2.16.jar   --jars /usr/local/spark/jars/postgresql-42.2.16.jar \
--total-executor-cores 24 \
--executor-cores 4 \
--executor-memory 10G \
--driver-memory 2g \
--py-files ./s3_to_spark/Calculation.py \
./App/userCommand.py $beginYear $endYear $filterNumber

echo "finsih" 
