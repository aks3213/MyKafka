package com.kafka.core;

import com.subscriber.Subscriber;
import lombok.Getter;

import java.util.concurrent.atomic.AtomicInteger;

@Getter
public class TopicSubscriber implements Runnable {
    private final AtomicInteger offset;
    private final Subscriber subscriber;
    private final Topic topic;

    public TopicSubscriber(final Topic topic, final Subscriber subscriber) {
        this.topic = topic;
        this.offset = new AtomicInteger(0);
        this.subscriber = subscriber;
    }

    @Override
    public void run() {
        synchronized (subscriber) {
            do {

                while (this.getOffset().get() >= this.getTopic().getMessages().size()) {
                    System.out.println("Putting thread to sleep for subscriber " + subscriber);
                    try {
                        subscriber.wait();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }

                this.getSubscriber().consume(this.getTopic().getMessages().get(this.getOffset().get()));

                this.getOffset().set(this.getOffset().get() + 1);

            } while (true);
        }
    }

    public void resetOffset(final int offSet) {
        System.out.println("Resetting the offset for " + subscriber);
        this.offset.set(offSet);
        wakeUpIfSleeping();
    }

    public void consumeMessages() {
    }

    public void wakeUpIfSleeping() {
        synchronized (subscriber) {
            System.out.println("Waking up thread for subscriber " + subscriber);
            subscriber.notify();
        }
    }

}
