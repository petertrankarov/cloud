package com.mapreduce.anagrams.cloud;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;

public class PubSubMessages {

    public static void publish (String projectId, String topicId, String message) throws IOException, ExecutionException, InterruptedException {

		TopicName topicName = TopicName.of(projectId, topicId);
		Publisher publisher = null;
		try {

			// create publisher instance
			publisher = Publisher.newBuilder(topicName).build();
			ByteString data = ByteString.copyFromUtf8(message);
			PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

			//return message id (unique in topic)
			ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
			String messageId = messageIdFuture.get();
			System.out.println("Published message ID: " + messageId);

		} finally {
			if (publisher != null) {
				//shutdown to free resources
				publisher.shutdown();
				publisher.awaitTermination(1, TimeUnit.MINUTES);
			}
		}
	}

    public static void subscribe (String projectId, String subscriptionId) {

        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);

        // New message receiver handles incoming messages
        MessageReceiver receiver = (PubsubMessage message, AckReplyConsumer consumer) -> {

            System.out.println("Id: " + message.getMessageId());
            System.out.println("Data: " + message.getData().toStringUtf8());
            consumer.ack();

            };

        Subscriber subscriber = null;
        try {

            // new subscriber, waiting 1 min for response
            subscriber = Subscriber.newBuilder(subscriptionName, receiver).build();
            subscriber.startAsync().awaitRunning();
            System.out.printf("Listening for messages on %s:\n", subscriptionName.toString());
            subscriber.awaitTerminated(1, TimeUnit.MINUTES);
        } catch (TimeoutException timeoutException) {
            // Shut down the subscriber. Stop receiving messages.
            subscriber.stopAsync();
        }


    }
    
}
