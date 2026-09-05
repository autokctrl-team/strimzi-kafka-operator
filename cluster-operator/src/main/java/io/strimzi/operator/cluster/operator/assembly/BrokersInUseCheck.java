/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.auth.Identity;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

/**
 * Class which contains several utility function which check if broker scale down or role change can be done or not.
 */
public class BrokersInUseCheck {
    private static final String CORDONED_LOG_DIRS = "cordoned.log.dirs";

    /**
     * Logger
     */
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(BrokersInUseCheck.class.getName());

    /**
     * Constructor
     */
    public BrokersInUseCheck() { }

    /**
     * Checks if broker contains any partition replicas when scaling down
     *
     * @param reconciliation        Reconciliation marker
     * @param coIdentity            Trust set and identity for authentication for connecting to the Kafka cluster
     * @param adminClientProvider   Used to create the Admin client instance
     *
     * @return returns CompletionStage with set of node ids containing partition replicas based on the outcome of the check
     */
    public CompletionStage<Set<Integer>> brokersInUse(Reconciliation reconciliation, Identity coIdentity, AdminClientProvider adminClientProvider) {
        try {
            String bootstrapHostname = KafkaResources.bootstrapServiceName(reconciliation.name()) + "." + reconciliation.namespace() + ".svc:" + KafkaCluster.REPLICATION_PORT;
            LOGGER.debugCr(reconciliation, "Creating AdminClient for Kafka cluster in namespace {}", reconciliation.namespace());
            Admin kafkaAdmin = adminClientProvider.createAdminClient(bootstrapHostname, coIdentity.trustSet(), coIdentity.authIdentity());

            return topicNames(kafkaAdmin)
                    .thenCompose(names -> describeTopics(kafkaAdmin, names))
                    .thenApply(topicDescriptions -> {
                        Set<Integer> brokersWithPartitionReplicas = new HashSet<>();

                        for (TopicDescription td : topicDescriptions.values()) {
                            for (TopicPartitionInfo pd : td.partitions()) {
                                for (Node broker : pd.replicas()) {
                                    brokersWithPartitionReplicas.add(broker.id());
                                }
                            }
                        }

                        return brokersWithPartitionReplicas;
                    }).whenComplete((result, error) -> {
                        if (error != null) {
                            LOGGER.warnCr(reconciliation, "Failed to get list of brokers in use", error);
                        }
                        kafkaAdmin.close();
                    });
        } catch (KafkaException e) {
            LOGGER.warnCr(reconciliation, "Failed to check if broker contains any partition replicas", e);
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Checks which brokers have all log directories cordoned.
     *
     * @param reconciliation        Reconciliation marker
     * @param coIdentity            Trust set and identity for authentication for connecting to the Kafka cluster
     * @param adminClientProvider   Used to create the Admin client instance
     * @param brokerIds             IDs of the brokers to check
     *
     * @return whether all requested brokers have {@code cordoned.log.dirs=*} in their effective configuration
     */
    public CompletionStage<Boolean> brokersCordoned(Reconciliation reconciliation, Identity coIdentity, AdminClientProvider adminClientProvider, Set<Integer> brokerIds) {
        try {
            String bootstrapHostname = KafkaResources.bootstrapServiceName(reconciliation.name()) + "." + reconciliation.namespace() + ".svc:" + KafkaCluster.REPLICATION_PORT;
            Admin kafkaAdmin = adminClientProvider.createAdminClient(bootstrapHostname, coIdentity.trustSet(), coIdentity.authIdentity());
            Set<ConfigResource> resources = brokerIds.stream()
                    .map(id -> new ConfigResource(ConfigResource.Type.BROKER, id.toString()))
                    .collect(Collectors.toSet());

            return kafkaAdmin.describeConfigs(resources).all().toCompletionStage()
                    .thenApply(configs -> configs.keySet().containsAll(resources)
                    && configs.values().stream().allMatch(config -> config.get(CORDONED_LOG_DIRS) != null
                        && "*".equals(config.get(CORDONED_LOG_DIRS).value())))
                    .whenComplete((result, error) -> kafkaAdmin.close());
        } catch (KafkaException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * This method gets the topic names after interacting with the Admin client
     *
     * @param kafkaAdmin          Instance of Kafka Admin
     * @return  a CompletionStage with set of topic names
     */
    /* test */ CompletionStage<Set<String>> topicNames(Admin kafkaAdmin) {
        return kafkaAdmin.listTopics(new ListTopicsOptions().listInternal(true)).names().toCompletionStage();
    }

    /**
     * Returns a collection of topic descriptions
     *
     * @param kafkaAdmin     Instance of Admin client
     * @param names          Set of topic names
     * @return a CompletionStage with map containing the topic name and description
     */
    /* test */ CompletionStage<Map<String, TopicDescription>> describeTopics(Admin kafkaAdmin, Set<String> names) {
        return kafkaAdmin.describeTopics(names).allTopicNames().toCompletionStage();
    }
}
