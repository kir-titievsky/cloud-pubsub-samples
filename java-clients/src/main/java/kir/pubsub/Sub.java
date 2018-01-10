package kir.pubsub;

import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.SubscriptionName;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import org.threeten.bp.Duration;


import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


public class Sub {

    // Instantiate an asynchronous message receiver

    public static void main(String... args) throws Exception {
        final String projectId = args[0];
        final String subscriptionId = args[1];
        final int timeoutMilliseconds = Integer.parseInt(args[2]);
        final int numPullers = Integer.parseInt(args[3]);
        final long maxOutstandingMessages = Integer.parseInt(args[4]);

        final AtomicInteger messageCount = new AtomicInteger(0);
        final DateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
        MessageReceiver receiver = new MessageReceiver() {
                    public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
                        int i = messageCount.incrementAndGet();
                        long timestampMs = message.getPublishTime().getSeconds()*1000 + message.getPublishTime().getNanos() / 1000000;
                        Date timestampDate = new Date(timestampMs);

                        try {
                            TimeUnit.MILLISECONDS.sleep(timeoutMilliseconds);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        System.out.println(String.format("t_log:%s\tt_pub:%s\tmessage:%s\tmessage_id:%s\tnum_messages:%d",
                                dateFormat.format(new Date()),
                                dateFormat.format(timestampDate),
                                message.getData().toStringUtf8(),
                                message.getMessageId(),
                                i
                        ));
                        consumer.ack();

                    }

                };

        SubscriptionName subscriptionName = SubscriptionName.of(projectId, subscriptionId);

        FlowControlSettings flowControlSettings =
                FlowControlSettings.newBuilder()
                        .setMaxOutstandingElementCount(maxOutstandingMessages).build();


        Subscriber subscriber = Subscriber
                .newBuilder(subscriptionName, receiver)
                .setFlowControlSettings(flowControlSettings)
                .setParallelPullCount(numPullers)
                .setExecutorProvider(InstantiatingExecutorProvider.newBuilder().setExecutorThreadCount(10).build())
                .build();
        subscriber.startAsync();
        subscriber.awaitRunning();
        System.out.println("Started async subscriber.");
        subscriber.awaitTerminated();
        System.out.println("Received " + messageCount.get() + " messages.");
    }
}