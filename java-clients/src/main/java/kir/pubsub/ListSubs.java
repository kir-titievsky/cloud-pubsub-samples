package kir.pubsub;
import com.google.cloud.pubsub.v1.PagedResponseWrappers;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.pubsub.v1.ListSubscriptionsRequest;
import com.google.pubsub.v1.ProjectName;
import com.google.pubsub.v1.Subscription;

public class ListSubs {
    public static void main(String[] args) throws Exception {
        SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create();

        ListSubscriptionsRequest listSubscriptionsRequest =
                ListSubscriptionsRequest.newBuilder()
                        .setProjectWithProjectName(ProjectName.of(String.valueOf(args[0])))
                        .build();
        PagedResponseWrappers.ListSubscriptionsPagedResponse response =
                subscriptionAdminClient.listSubscriptions(listSubscriptionsRequest);
        Iterable<Subscription> subscriptions = response.iterateAll();
        for (Subscription subscription : subscriptions) {
            // do something with the subscription
            System.out.println(subscription.getName());
        }
    }
}
