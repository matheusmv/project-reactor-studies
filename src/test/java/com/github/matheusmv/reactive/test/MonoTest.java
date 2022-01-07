package com.github.matheusmv.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.UUID;

@Slf4j
public class MonoTest {

    @Test
    public void monoSubscriber() {
        var uuid = UUID.randomUUID().toString();
        var mono = Mono.just(uuid).log();

        mono.subscribe();

        StepVerifier.create(mono).expectNext(uuid).verifyComplete();
    }

    @Test
    public void monoSubscriberWithConsumer() {
        var uuid = UUID.randomUUID().toString();
        var mono = Mono.just(uuid).log();

        mono.subscribe(s -> log.info("Value {}", s));

        StepVerifier.create(mono).expectNext(uuid).verifyComplete();
    }

    @Test
    public void monoSubscriberWithConsumerError() {
        var uuid = UUID.randomUUID().toString();
        var mono = Mono.just(uuid)
                .map(s -> {
                    throw new RuntimeException("Testing mono with error");
                });

        mono.subscribe(
                s -> log.info("Value {}", s),
                s -> log.error("Something went wrong")
        );

        mono.subscribe(
                s -> log.info("Value {}", s),
                Throwable::printStackTrace
        );

        StepVerifier.create(mono).expectError(RuntimeException.class).verify();
    }

    @Test
    public void monoSubscriberWithConsumerComplete() {
        var uuid = UUID.randomUUID().toString();
        var mono = Mono.just(uuid).map(String::toUpperCase);

        mono.subscribe(
                s -> log.info("Value {}", s),
                Throwable::printStackTrace,
                () -> log.info("finished.")
        );

        StepVerifier.create(mono).expectNext(uuid.toUpperCase()).verifyComplete();
    }

    @Test
    public void monoSubscriberWithConsumerSubscription() {
        var uuid = UUID.randomUUID().toString();
        var mono = Mono.just(uuid).map(String::toUpperCase);

        mono.subscribe(
                s -> log.info("Value {}", s),
                Throwable::printStackTrace,
                () -> log.info("finished."),
                Subscription::cancel
        );

        StepVerifier.create(mono).expectNext(uuid.toUpperCase()).verifyComplete();
    }

    @Test
    public void monoSubscriberWithConsumerSubscriptionBackpressure() {
        var uuid = UUID.randomUUID().toString();
        var mono = Mono.just(uuid).map(String::toUpperCase);

        mono.subscribe(
                s -> log.info("Value {}", s),
                Throwable::printStackTrace,
                () -> log.info("finished."),
                subscription -> subscription.request(5)
        );

        StepVerifier.create(mono).expectNext(uuid.toUpperCase()).verifyComplete();
    }
}
