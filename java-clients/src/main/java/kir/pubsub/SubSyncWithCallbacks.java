package kir.pubsub;

import com.google.common.collect.Iterables;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;

import com.google.common.util.concurrent.*;
import com.google.pubsub.v1.*;
import org.threeten.bp.Duration;


import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import static java.lang.Math.toIntExact;
import static java.util.concurrent.TimeUnit.SECONDS;

public class SubSyncWithCallbacks{

    private static GrpcSubscriberStub buildSubscriberStub(int maxRpcSeconds) throws IOException{

        SubscriptionAdminSettings.Builder clientSettingsBuilder = SubscriptionAdminSettings.newBuilder();
        // The goal here is to minimize time retrying individual requests, since we
        // are retrying them in a loop anyway.  By default, each request will hang for
        // up to 10 minutes. We want to limit the amount of time we wait.
        clientSettingsBuilder.pullSettings().setRetrySettings(
                clientSettingsBuilder.pullSettings().getRetrySettings().toBuilder()
                        .setInitialRpcTimeout(Duration.ofSeconds(1))
                        .setMaxAttempts(1)
                        .build());

        // for acks and modAcks it is worth waiting a bit longer than for pulls
        clientSettingsBuilder.acknowledgeSettings().setRetrySettings(
                clientSettingsBuilder.acknowledgeSettings().getRetrySettings().toBuilder()
                        .setInitialRpcTimeout(Duration.ofSeconds(1))
                        .setTotalTimeout(Duration.ofSeconds(maxRpcSeconds))
                        .build());
        clientSettingsBuilder.modifyAckDeadlineSettings()
                .setRetrySettings(clientSettingsBuilder.acknowledgeSettings().getRetrySettings().toBuilder()
                        .setInitialRpcTimeout(Duration.ofSeconds(1))
                        .setTotalTimeout(Duration.ofSeconds(maxRpcSeconds))
                        .build());

        return GrpcSubscriberStub.create(clientSettingsBuilder.build());
    }

    public static void main(String... args) throws Exception {
        final String projectId = args[0];
        final String subscriptionId = args[1];
        final int pullIntervalSeconds = Integer.parseInt(args[2]);
        final int numThreads = Integer.parseInt(args[3]);
        final int timeoutMilliseconds = Integer.parseInt(args[4]);


        // Messages are processed in two stages. Once we receive a message
        // it is scheduled for processing and enters the toModAck -- the set of
        // messages for which to continue extending the ackDeadline, while processing happens.
        // Once the message has been successfully processed we also add it to the
        // toAck set, which schedules the message for the acknowledgement.
        // Once the acknowledgements are successfully processed, the message is removed
        // from both sets.  Practically, the sets contain ackIds rather than messages themselves.
        final Set<String> toAck = ConcurrentHashMap.newKeySet();
        final Set<String> toModAck = ConcurrentHashMap.newKeySet();

        int sendAckPeriodSeconds = 1;

        // this is how long we give the RPC to complete.  This is particularly important for pull
        // where there may not be any messages to deliver.
        int maxRpcSeconds = 20;
        // When messages are being processed, we want to extend the deadline for them regularly
        // by some amount that is ideally the amount of time required to finish processing the message and
        // successfully send the ack request and for it to be received by Pub/Sub.
        // We estimate this number here.  This can be optimized quite a bit.
        int ackDeadlineSecondsIncrement = Math.min(Math.max(10, timeoutMilliseconds/1000*2), 10*60*60);
        // We send modAck requests no more frequently than 1/second
        int sendModAckPeriodSeconds = Math.max(1, ackDeadlineSecondsIncrement/2);

        // Worker pool for processing messages. We should not pull more messages than available threads.
        // The listening decorator simplifies handling of callbacks when messages are processed.
        ListeningExecutorService workerPool =
                MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(toIntExact(numThreads)));
        // thread pool used for managing acks
        ScheduledExecutorService servicePool = Executors.newScheduledThreadPool(4);

        // create constant bits of request bodies used for pull, ack, and modAckRequests
        String subName = SubscriptionName.of(projectId, subscriptionId).toString();
        PullRequest.Builder pullRequestBuilder =
                PullRequest.newBuilder()
                        .setReturnImmediately(Boolean.FALSE)
                        .setSubscription(subName);
        ModifyAckDeadlineRequest.Builder modAckRequestBuilder = ModifyAckDeadlineRequest.newBuilder()
                .setAckDeadlineSeconds(ackDeadlineSecondsIncrement)
                .setSubscription(subName);
        AcknowledgeRequest.Builder ackRequestBuilder = AcknowledgeRequest.newBuilder()
                .setSubscription(subName);

        // now build a pub/sub client that can send these request objects and retrieve results.
        GrpcSubscriberStub subscriber = buildSubscriberStub(maxRpcSeconds);

        // Now set various loops to actually pull, modAck, and ackMessages.
        // Also, we need a loop to monitor worker threads for completion.
        // There are four threads running synchronous, blocking RPCs
        // that can take seconds. So we must make sure that the workerPool has at least four threds.


        // pull messages regularly and scheduling them for processing
        servicePool.scheduleWithFixedDelay(()-> {
            // do nothing if we have no free threads to process new messages
            // we could optimize this by dynamically adjusting the max number of messages to pull
            // to the number of available worker threads.
            if (toModAck.size() >= numThreads) {
                return;
            }
            // grab up to as many messages as there are free threads in the worker pool
            PullResponse pullResponse = subscriber.pullCallable().call(
                    pullRequestBuilder.setMaxMessages(numThreads - toModAck.size()).build());
            for (ReceivedMessage message : pullResponse.getReceivedMessagesList()) {
                // we keep track of message processing in an array message id
                ListenableFuture<String> future =
                        workerPool.submit(new MessageHandler(message.getMessage(), timeoutMilliseconds));
                final String ackId = message.getAckId();
                toModAck.add(ackId);
                Futures.addCallback(future, new FutureCallback<String>() {
                    public void onSuccess(String message_id) {
                        toAck.add(ackId);
                    }
                    public void onFailure(Throwable thrown) {
                        // we've failed to process this message, so stop modAcking it so it gets redelivered
                        toModAck.remove(ackId);
                        thrown.printStackTrace();
                    }
                });
            }
        },0,pullIntervalSeconds, SECONDS);

        // schedule sending of modifyAckDeadline requests for modifyAckDeadline for outstanding
        servicePool.scheduleWithFixedDelay(() -> {
            // this may fail, but that's OK, the message will simply get re-delivered otherwise
            if (! toModAck.isEmpty()) {
                for (List<String> ackIds : Iterables.partition(toModAck, 1000)) {
                    subscriber.modifyAckDeadlineCallable().
                            call(modAckRequestBuilder.addAllAckIds(ackIds).build());
                    System.out.println("Sent " + toModAck.size() + " modAcks.");
                }
            }
        }, sendModAckPeriodSeconds/2, sendModAckPeriodSeconds,SECONDS);

        // schedule sending of outstanding acks for messages already processed
        servicePool.scheduleWithFixedDelay(() -> {
            // TODO: Handle cases with more than 1000 Acks to send -- we don't allow batches larger than that
            // this may fail, but that's OK, the message will simply get re-delivered otherwise
            if (! toAck.isEmpty()) {
                HashSet<String> localAckList = new HashSet<>(toAck);
                // the maximum batch size is 1000 ackIds, so in case we end up with more than
                // a 1000 to send, we must break up the set
                for (List<String> ackIds : Iterables.partition(localAckList, 1000)) {
                    subscriber.acknowledgeCallable().call(ackRequestBuilder.addAllAckIds(ackIds).build());
                    toAck.removeAll(ackIds);
                    // it is now safe to stop extending ackDeadlines for the acked messages
                    toModAck.removeAll(ackIds);
                    System.out.println("Sent " + localAckList.size() + " acks.");
                }
            }
        }, 0, sendAckPeriodSeconds,SECONDS);
    }
}