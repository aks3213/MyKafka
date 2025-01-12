package com.kafka.core;

import com.kafka.model.Message;

public interface ISubscriber {
    void consume(Message message);
}
