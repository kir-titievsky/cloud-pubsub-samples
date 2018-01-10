package kir.pubsub;

import com.google.api.core.ApiFuture;
// import com.google.api.core.ApiFutureCallback;
// import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.BatchingSettings;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;

import org.threeten.bp.Duration;

import java.util.concurrent.TimeUnit;

public class Pub {

    //schedule a message to be published, messages are automatically batched
    private static ApiFuture<String> publishMessage(Publisher publisher, String message)
            throws Exception {
        // convert message to bytes
        ByteString data = ByteString.copyFromUtf8(message);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
        //TimeUnit.MILLISECONDS.sleep(100);
        return publisher.publish(pubsubMessage);
    }
    public static void main(String[] args) throws Exception{
        // topic id, eg. "my-topic"
        String projectId = ServiceOptions.getDefaultProjectId();
        String topicId = args[0];
        Integer numMessages = Integer.valueOf(args[1]);
        Long messageBatchSize = Long.valueOf(args[2]);
        TopicName topicName = TopicName.of(projectId, topicId);
        Publisher publisher = null;

        BatchingSettings batchingSettings = BatchingSettings.newBuilder()
                .setDelayThreshold(Duration.ofMillis(100))
                .setRequestByteThreshold(10000L)
                .setElementCountThreshold(messageBatchSize)
                .build();
        publisher = Publisher.newBuilder(topicName).setBatchingSettings(batchingSettings).build();

        for (int i = 0; i < numMessages; i++) {
            try {
                publishMessage(publisher, String.valueOf(i));
                if ( (i % 100) == 0 ) {
                    System.out.println("published message " + i);
                }
            }
            catch(Exception e){
                System.out.println("Failed to publish message with error " + e.getMessage() + e.getStackTrace());
            }
            // outstandingMessages.add(String.valueOf(i));
        }
        publisher.shutdown();
    }
}
