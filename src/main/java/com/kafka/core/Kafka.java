package com.kafka.core;

import com.subscriber.Subscriber;
import com.kafka.model.Message;
import lombok.Getter;

import java.util.HashMap;

@Getter
public class Kafka {
    private final HashMap<String, Topic> topicIdToTopic;

    public Kafka() {
        topicIdToTopic = new HashMap<>();
    }

    public void addTopic(final Topic topic) {
        topicIdToTopic.put(topic.getTopicId(), topic);
    }

    public void subscribeToTopic(final String topicId, final Subscriber subscriber) {
        topicCheck(topicId);

        final Topic topic = topicIdToTopic.get(topicId);

        topic.subscribe(subscriber);
    }

    public void publishMessageToTopic(final String topicId, final Message message) {

        topicCheck(topicId);

        final Topic topic = topicIdToTopic.get(topicId);

        topic.publishMessage(message);
    }

    public void resetOffset(final String topicId, final String subscriberId) {
        topicCheck(topicId);

        final Topic topic = topicIdToTopic.get(topicId);

        topic.resetOffset(subscriberId);
    }

    private void topicCheck(final String topicId) {
        if (!topicIdToTopic.containsKey(topicId)) {
            throw new RuntimeException("Topic does not exist");
        }
    }
}
