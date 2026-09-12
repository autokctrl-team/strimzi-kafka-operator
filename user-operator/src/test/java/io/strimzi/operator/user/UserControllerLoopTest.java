/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserList;
import io.strimzi.api.kafka.model.user.KafkaUserStatus;
import io.strimzi.operator.common.MicrometerMetricsProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.controller.ControllerQueue;
import io.strimzi.operator.common.controller.ReconciliationLockManager;
import io.strimzi.operator.common.metrics.ControllerMetricsHolder;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.StatusUtils;
import io.strimzi.operator.common.operator.resource.kubernetes.CrdOperator;
import io.strimzi.operator.common.operator.resource.kubernetes.Informer;
import io.strimzi.operator.user.operator.KafkaUserOperator;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class UserControllerLoopTest {
    private static final String NAMESPACE = "namespace";
    private static final String NAME = "user";

    @Test
    public void testStatusNotUpdatedAfterSameNameReplacement() {
        KafkaUser originalUser = ResourceUtils.createKafkaUserTls(NAMESPACE);
        originalUser.getMetadata().setGeneration(2L);
        originalUser.getMetadata().setUid("original-uid");

        KafkaUser replacementUser = ResourceUtils.createKafkaUserTls(NAMESPACE);
        replacementUser.getMetadata().setGeneration(1L);
        replacementUser.getMetadata().setUid("replacement-uid");

        KafkaUserStatus status = new KafkaUserStatus();
        StatusUtils.setStatusConditionAndObservedGeneration(originalUser, status, (Throwable) null);

        Informer<KafkaUser> userInformer = mock();
        Informer<Secret> secretInformer = mock();
        CrdOperator<KubernetesClient, KafkaUser, KafkaUserList> userCrdOperator = mock();
        KafkaUserOperator userOperator = mock();
        ControllerMetricsHolder metrics = new ControllerMetricsHolder(
                KafkaUser.RESOURCE_KIND,
                Labels.EMPTY,
                new MicrometerMetricsProvider(new SimpleMeterRegistry()));
        UserOperatorConfig config = ResourceUtils.createUserOperatorConfigForUserControllerTesting(NAMESPACE, Map.of(), 120000, 10, 1, "");

        when(userInformer.get(NAMESPACE, NAME)).thenReturn(originalUser, replacementUser);
        when(userOperator.reconcile(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(status));

        UserControllerLoop controllerLoop = new UserControllerLoop(
                "test-controller-loop",
                new ControllerQueue(10, metrics),
                new ReconciliationLockManager(),
                mock(ScheduledExecutorService.class),
                userInformer,
                secretInformer,
                userCrdOperator,
                userOperator,
                metrics,
                config);

        controllerLoop.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, NAMESPACE, NAME));

        verify(userCrdOperator, never()).updateStatusAsync(any(), any());
    }
}
