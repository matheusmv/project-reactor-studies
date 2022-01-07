package com.github.matheusmv.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.blockhound.BlockHound;
import reactor.blockhound.BlockingOperationError;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.UUID;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

@Slf4j
public class MonoTest {

    @BeforeAll
    public static void setUp() {
        BlockHound.install();
    }

    @Test
    public void blockHoundWorks() {
        try {
            var task = new FutureTask<>(() -> {
                Thread.sleep(0);
                return "";
            });
            Schedulers.parallel().schedule(task);

            task.get(10, TimeUnit.SECONDS);
            Assertions.fail("should fail");
        } catch (Exception e) {
            Assertions.assertTrue(e.getCause() instanceof BlockingOperationError);
        }
    }

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

    @Test
    public void monoDoOnMethods() {
        var uuid = UUID.randomUUID().toString();
        var mono = Mono.just(uuid)
                .map(String::toUpperCase)
                .doOnSubscribe(subscription -> log.info("Subscribed"))
                .doOnRequest(value -> log.info("Request received"))
                .doOnNext(s -> log.info("Value is here {}", s))
                .flatMap(s -> Mono.empty())
                .doOnNext(s -> log.info("Value is here {}", s))  // not executed
                .doOnSuccess(s -> log.info("Successfully executed. Value {}", s));

        mono.subscribe(
                s -> log.info("Value {}", s),
                Throwable::printStackTrace,
                () -> log.info("finished.")
        );
    }

    @Test
    public void monoDoOnError() {
        var error = Mono.error(new IllegalArgumentException("Illegal argument"))
                .doOnError(e -> log.error("Error message: {}", e.getMessage()))
                .doOnNext(s -> log.info("next {}", s))   // not executed
                .log();

        StepVerifier.create(error).expectError(IllegalArgumentException.class).verify();
    }

    @Test
    public void monoOnErrorResume() {
        var uuid = UUID.randomUUID().toString();
        var error = Mono.error(new IllegalArgumentException("Illegal argument"))
                .doOnError(e -> log.error("Error message: {}", e.getMessage()))
                .onErrorResume(s -> {
                    log.error("onErrorResume");
                    return Mono.just(uuid);
                })
                .log();

        StepVerifier.create(error).expectNext(uuid).verifyComplete();
    }

    @Test
    public void monoOnErrorReturn() {
        var uuid = UUID.randomUUID().toString();
        var error = Mono.error(new IllegalArgumentException("Illegal argument"))
                .doOnError(e -> log.error("Error message: {}", e.getMessage()))
                .onErrorReturn(uuid)
                .log();

        StepVerifier.create(error).expectNext(uuid).verifyComplete();
    }
}
