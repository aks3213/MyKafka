package com.subscriber;

import com.kafka.core.ISubscriber;
import com.kafka.model.Message;
import lombok.Getter;

@Getter
public class Subscriber implements ISubscriber {
    private final String subscriberId;
    private final String subscriberName;

    public Subscriber(final String subscriberId, final String subscriberName) {
        this.subscriberId = subscriberId;
        this.subscriberName = subscriberName;
    }

    public void consume(final Message message) {
        System.out.println("Subscriber " + subscriberId + " read message " + message);
    }

}
