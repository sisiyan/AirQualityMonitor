spark-submit --master spark://ec2-34-192-116-49.compute-1.amazonaws.com:7077 \
             --jars /usr/share/java/mysql-connector-java-8.0.12.jar \
             --driver-memory 4G \
             --executor-memory 4G \
             /home/ubuntu/insightProject/src/spark/
