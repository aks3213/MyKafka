package com.kafka;

import com.kafka.core.Kafka;
import com.subscriber.Subscriber;
import com.kafka.core.Topic;
import com.kafka.model.Message;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello, World!");

        final Kafka kafka = new Kafka();

        final Topic topic1 = new Topic("topic-id-1", "topic-name-1");
        final Topic topic2 = new Topic("topic-id-2", "topic-name-2");

        final Subscriber subscriber1 = new Subscriber("subscriber-id-1", "subscriber-name-1");
        final Subscriber subscriber2 = new Subscriber("subscriber-id-2", "subscriber-name-2");
        final Subscriber subscriber3 = new Subscriber("subscriber-id-3", "subscriber-name-3");

        kafka.addTopic(topic1);
        kafka.addTopic(topic2);

        kafka.subscribeToTopic("topic-id-1", subscriber1);
        kafka.subscribeToTopic("topic-id-1", subscriber2);
        kafka.subscribeToTopic("topic-id-2", subscriber3);

        kafka.publishMessageToTopic("topic-id-1", new Message("message-id-1", "message-1"));
        kafka.publishMessageToTopic("topic-id-1", new Message("message-id-2", "message-2"));
        kafka.publishMessageToTopic("topic-id-2", new Message("message-id-3", "message-3"));

        kafka.resetOffset(topic1.getTopicId(), subscriber1.getSubscriberId()); // subscriber should consume all message from topic 1 again


    }
}