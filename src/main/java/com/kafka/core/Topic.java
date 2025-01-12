package com.kafka.core;

import com.kafka.model.Message;
import com.subscriber.Subscriber;
import lombok.Getter;
import lombok.ToString;

import java.util.*;

@Getter
@ToString
public class Topic {
    private final String topicId;
    private final String topicName;
    private final List<Message> messages;
    private final HashMap<String, TopicSubscriber> subscriberIdToTopicSubscriber;

    public Topic(final String topicId, final String topicName) {
        this.topicId = topicId;
        this.topicName = topicName;
        this.messages = Collections.synchronizedList(new ArrayList<>());
        this.subscriberIdToTopicSubscriber = new HashMap<>();
    }

    public void publishMessage(final Message message) {

        System.out.println("Added message to topic message " + message + " to the topic " + topicId);

        messages.add(message);

        subscriberIdToTopicSubscriber.values().forEach((TopicSubscriber::wakeUpIfSleeping));
    }

    public void subscribe(Subscriber subscriber) {
        final TopicSubscriber topicSubscriber = new TopicSubscriber(this, subscriber);
        Thread t = new Thread(topicSubscriber);
        t.start();
        subscriberIdToTopicSubscriber.put(subscriber.getSubscriberId(), topicSubscriber);
    }

    public void resetOffset(String subscriberId) {
        if (!subscriberIdToTopicSubscriber.containsKey(subscriberId)) {
            return;
        }
        final TopicSubscriber topicSubscriber = subscriberIdToTopicSubscriber.get(subscriberId);
        topicSubscriber.resetOffset(0);
        System.out.println("Topic sub offset updated to " + topicSubscriber.getOffset());
        topicSubscriber.wakeUpIfSleeping();
    }
}
