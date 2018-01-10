package kir.pubsub;

import com.google.pubsub.v1.*;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import org.threeten.bp.Duration;


import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

import static java.lang.Math.toIntExact;
import static java.util.concurrent.TimeUnit.SECONDS;

class MessageHandler implements Callable<String>{
    private PubsubMessage message;
    private long timeoutMilliseconds;
    final DateFormat DATE_FORMAT = new SimpleDateFormat("HH:mm:ss.SSS");

    MessageHandler(PubsubMessage message, long timeoutMilliseconds){
        this.message = message;
        this.timeoutMilliseconds = timeoutMilliseconds;
    }
    public String call(){
        long timestampMs = this.message.getPublishTime().getSeconds() * 1000
                + this.message.getPublishTime().getNanos() / 1000000;
        Date timestampDate = new Date(timestampMs);
        try {
            TimeUnit.MILLISECONDS.sleep(this.timeoutMilliseconds);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(String.format("t_log:%s\tt_pub:%s\tmessage:%s\tmessage_id:%s",
                DATE_FORMAT.format(new Date()),
                DATE_FORMAT.format(timestampDate),
                this.message.getData().toStringUtf8(),
                this.message.getMessageId()
        ));
        return message.getMessageId();
    }
}

public class SubSyncV1{
    
    private static GrpcSubscriberStub buildSubcriberStub(int maxRpcSeconds) throws IOException{
        SubscriptionAdminSettings.Builder clientSettingsBuilder = SubscriptionAdminSettings.newBuilder();

        clientSettingsBuilder.pullSettings().setRetrySettings(
                clientSettingsBuilder.pullSettings().getRetrySettings().toBuilder()
                        .setTotalTimeout(Duration.ofSeconds(maxRpcSeconds)).build());
        clientSettingsBuilder.acknowledgeSettings().setRetrySettings(
                clientSettingsBuilder.pullSettings().getRetrySettings().toBuilder()
                        .setTotalTimeout(Duration.ofSeconds(maxRpcSeconds)).build());
        clientSettingsBuilder.modifyAckDeadlineSettings()
                .setRetrySettings(clientSettingsBuilder.pullSettings().getRetrySettings().toBuilder()
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


        // this is hash map of futures for processing messages
        // when messages are scheduled processing they are added to this set
        // When processing is done, we move them to the toAck set
        // and when we've successfully acked the messages, we discard any information about them
        final ConcurrentHashMap<String,Future<String>> outstandingMessages = new ConcurrentHashMap<>();
        final Set<String> toAck = ConcurrentHashMap.newKeySet();

        // we want to check on task completions every second or so.. Maybe faster for shorter tasks.
        int taskCompletionCheckPeriodSeconds = 1;
        int sendAckPeriodSeconds = 1;
        // this is how long we give the RPC to complete.  This is particularly important for pull
        // where there may not be any messages to deliver.
        int maxRpcSeconds = 5;
        // When messages are being processed, we want to extend the deadline for them regularly
        // by some amount that is ideally the amount of time required to finish processing the message and
        // successfully send the ack request and for it to be received by Pub/Sub.
        // We estimate this number here.  This can be optimized quite a bit.
        int ackDeadlineSeconds = Math.min(Math.max(10, timeoutMilliseconds/1000*2), 10*60*60);

        // worker pool for processing messages. We should not pull more messages than available threads.
        ExecutorService workerPool = Executors.newFixedThreadPool(toIntExact(numThreads));
        // thread pool used for managing acks
        ScheduledExecutorService servicePool = Executors.newScheduledThreadPool(4);


        // create constant bits of request bodies used for pull, ack, and modAckRequests
        String subName = SubscriptionName.of(projectId, subscriptionId).toString();
        PullRequest pullRequest =
                PullRequest.newBuilder()
                            .setMaxMessages(toIntExact(numThreads))
                            .setReturnImmediately(Boolean.FALSE)
                            .setSubscription(subName)
                            .build();
        ModifyAckDeadlineRequest.Builder modAckRequestBuilder = ModifyAckDeadlineRequest.newBuilder()
                .setAckDeadlineSeconds(ackDeadlineSeconds)
                .setSubscription(subName);
        AcknowledgeRequest.Builder ackRequestBuilder = AcknowledgeRequest.newBuilder()
                .setSubscription(subName);

        // now build a pub/sub client that can send these request objects and retrieve results.
        GrpcSubscriberStub subscriber = buildSubcriberStub(maxRpcSeconds);

        // Now set various loops to actually pull, modAck, and ackMessages.
        // Also, we need a loop to monitor worker threads for completion.
        // There are four threads running synchronous, blocking RPCs
        // that can take seconds. So we must make sure that the workerPool has at least four threds.

        // schedule sending of modifyAckDeadline requests for modifyAckDeadline for outstanding
        servicePool.scheduleWithFixedDelay(() -> {
            // TODO: Handle cases with more than 1000 modAcks
            // this may fail, but that's OK, the message will simply get re-delivered otherwise
            if (outstandingMessages.isEmpty()){
                return;
            }
            subscriber.modifyAckDeadlineCallable().
                    call(modAckRequestBuilder.addAllAckIds(outstandingMessages.keySet()).build());
        }, 0, sendAckPeriodSeconds,SECONDS);

        // pull messages regularly and scheduling them for processing
        servicePool.scheduleWithFixedDelay(()-> {
            // do nothing if we have no free threads to process new messages
            // we could optimize this by dynamically adjusting the max number of messages to pull
            // to the number of available worker threads.
            if (outstandingMessages.size() >= numThreads) {
                return;
            }
            PullResponse pullResponse = subscriber.pullCallable().call(pullRequest);
            for (ReceivedMessage message : pullResponse.getReceivedMessagesList()) {
                // we keep track of message processing in an array message id
                outstandingMessages.put(
                        message.getAckId(),
                        workerPool.submit(new MessageHandler(message.getMessage(), timeoutMilliseconds))
                );
            }},0,pullIntervalSeconds, SECONDS);

        // Now we need to keep track of the futures crated by workerPool submit to
        // find out when message processing completes or needs extra time.
        // Based on the results we populate the toAck and toModAck sets.
        // This can be done much more efficiently with CompletableFutures or ListenableFutures,
        // but this is straightforward.
        servicePool.scheduleWithFixedDelay(()-> {
            for (Map.Entry<String, Future<String>> messageTask : outstandingMessages.entrySet()) {
                String ackId = messageTask.getKey();
                if (messageTask.getValue().isDone()) {
                    try {
                        // get should throw an error if the processing did not actually succeed
                        messageTask.getValue().get();
                        // we need to schedule the message for an ack and stop modAcking it
                        toAck.add(ackId);
                        outstandingMessages.remove(ackId);
                    } catch (Exception e) {
                        // this means that the task has not succeeded completely
                        // and we should attempt reprocessing the message
                        // we do this by letting the message modAck expire,so it is re-delivered later
                        //System.err.println("Caught a completed task that failed with: ");
                        e.printStackTrace();
                    }
                } else {
                    // make sure we haven't accidentally scheduled the message to be acked
                    toAck.remove(ackId);
                }
            }
        },0,taskCompletionCheckPeriodSeconds, SECONDS);

        // schedule sending of outstanding acks for messages already processed
        servicePool.scheduleWithFixedDelay(() -> {
            // TODO: Handle cases with more than 1000 Acks to send -- we don't allow batches larger than that
            // this may fail, but that's OK, the message will simply get re-delivered otherwise
            if (toAck.isEmpty()){
                return;
            }
            HashSet<String> localAckList = new HashSet<>(toAck);
            subscriber.acknowledgeCallable().call(ackRequestBuilder.addAllAckIds(localAckList).build());
            toAck.removeAll(localAckList);
        }, 0, sendAckPeriodSeconds,SECONDS);
    }
}