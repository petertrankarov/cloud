package com.mapreduce.anagrams.cloud;

import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

@Component
public class pubsubReceiver implements MessageReceiver {

    private Log log = LogFactory.getLog(pubsubReceiver.class);
    @Override
    public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
        log.info("Message Details: ");
        log.info("ID: " + message.getMessageId());
        log.info("Data: " + message.getData().toStringUtf8());
        consumer.ack();
    }
    
}
