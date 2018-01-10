package kir.pubsub;

import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.pubsub.v1.*;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.AckReplyConsumer;

import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;


import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

import static java.lang.Math.toIntExact;


public class SubSync{

    // Instantiate an asynchronous message receiver
    final DateFormat DATE_FORMAT = new SimpleDateFormat("HH:mm:ss.SSS");
    public ListenableFuture<String> processMessage(PubsubMessage message, Long timeoutMilliseconds) {
        SettableFuture<String> toReturn = SettableFuture.create();
        try {
            long timestampMs = message.getPublishTime().getSeconds() * 1000
                    + message.getPublishTime().getNanos() / 1000000;
            Date timestampDate = new Date(timestampMs);
            try {
                TimeUnit.MILLISECONDS.sleep(timeoutMilliseconds);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println(String.format("t_log:%s\tt_pub:%s\tmessage:%s\tmessage_id:%s\tnum_messages:%d",
                    DATE_FORMAT.format(new Date()),
                    DATE_FORMAT.format(timestampDate),
                    message.getData().toStringUtf8(),
                    message.getMessageId()
            ));
            toReturn.set(message.getMessageId());
        } catch (Throwable t) {
            toReturn.setException(t);
        }
        return toReturn;
    };

    public static void main(String... args) throws Exception {
        final String projectId = args[0];
        final String subscriptionId = args[1];
        final long timeoutMilliseconds = Integer.parseInt(args[2]);
        final long numPullers = Integer.parseInt(args[3]);
        final long maxOutstandingMessages = Integer.parseInt(args[4]);
        final Set<String> toModAck = ConcurrentHashMap.newKeySet();
        final Set<String> toAck = ConcurrentHashMap.newKeySet();

        SubscriptionAdminSettings subscriptionAdminSettings =
                SubscriptionAdminSettings.newBuilder().build();
        ExecutorService workerPool = Executors.newFixedThreadPool(toIntExact(maxOutstandingMessages));
        ScheduledExecutorService servicePool = Executors.newSingleThreadScheduledExecutor();

        SubscriberStub subscriber
                = GrpcSubscriberStub.create(subscriptionAdminSettings);

        // periodically send acknowledgements for messages
        // note that the acknowledgementDeadline should be set to > ACK_PERIOD
        int ACK_PERIOD_SECONDS = 20;
        servicePool.scheduleWithFixedDelay(() -> {
            HashSet<String> tmpToAck;
            HashSet<String> tmpToModAck;
            synchronized (toAck) {
                tmpToAck = new HashSet<>(toAck);
                tmpToModAck = new HashSet<>(toModAck);
                toAck.clear();
            }
            try {
                subscriber.acknowledgeCallable().call(AcknowledgeRequest.newBuilder().addAllAckIds(tmpToAck).build());
            } catch (Throwable t) {
                synchronized (toAck) {
                    toAck.addAll(tmpToAck);
                }
            }
            subscriber.modifyAckDeadlineCallable()
                        .call(
                                ModifyAckDeadlineRequest.newBuilder()
                                .addAllAckIds(tmpToModAck)
                                .setAckDeadlineSeconds(30)
                                .build()
                        );
        }, ACK_PERIOD_SECONDS, ACK_PERIOD_SECONDS, TimeUnit.SECONDS);

        try {
            SubscriptionName subName = SubscriptionName.of(projectId, subscriptionId);
            PullRequest pullRequest =
                    PullRequest.newBuilder()
                            .setMaxMessages(toIntExact(maxOutstandingMessages))
                            .setReturnImmediately(Boolean.FALSE) // wait until we have a maxOutsandingMessages
                            .setSubscription(subName.toString())
                            .build();

            // use pullCallable().futureCall to asynchronously perform this operation
            PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);
            for (ReceivedMessage message : pullResponse.getReceivedMessagesList()) {
                toAck.add(message.getAckId());
                workerPool.submit(() -> {
                   /*
                   Futures.addCallback(new MessageHandler(message.getMessage()).get(),

                   new FutureCallback<String>() {
                       @Override
                       public void onSuccess(String s) {
                           toAck.add(message.getAckId());
                           toModAck.remove(message.getAckId());
                       }
                       @Override
                       // let the message expire and be re-delivered
                       // it may be nacked to speed this up
                       public void onFailure(Throwable t) {
                               toModAck.remove(message.getAckId());
                       }
                       */
                });
            }
        }
        finally {
            // crash
        }
    }
}