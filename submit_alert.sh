nohup java -jar \
-Dlog4j.configuration=file:log4j.properties \
target/KafkaVesAlerting-1.0-SNAPSHOT.jar \
>/dev/null 2>&1 &
