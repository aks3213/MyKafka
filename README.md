Kafka basic implementation:
* Users should be able to create topics
* Publishers should be able to publish messages to a topic
* Consumers should be able to subscribe to topics
* Consumers should be able to receive messages from a subscribed topic
* Consumers should be able to reset offset for a subscribed topic to receive all messages again

How to run:
* change source and target version in pom.xml based on your java version
* build the package with `mvn clean install`
* run the jar with `java -cp target/MyKafka-1.0-SNAPSHOT.jar com.kafka.Main `