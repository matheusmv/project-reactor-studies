package com.github.matheusmv.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;

@Slf4j
public class FluxTest {

    @Test
    public void fluxSubscriber() {
        var flux = Flux.just("A", "B", "C", "D", "E").log();

        StepVerifier.create(flux).expectNext("A", "B", "C", "D", "E").verifyComplete();
    }

    @Test
    public void fluxSubscriberNumbers() {
        var flux = Flux.range(1, 5);

        flux.subscribe(
                i -> log.info("Number {}", i)
        );

        StepVerifier.create(flux).expectNext(1, 2, 3, 4, 5).verifyComplete();
    }

    @Test
    public void fluxSubscriberFromList() {
        var listOfIntegers = List.of(1, 2, 3, 4, 5);
        var flux = Flux.fromIterable(listOfIntegers);

        flux.subscribe(
                i -> log.info("List {}", i)
        );

        StepVerifier.create(flux).expectNext(1, 2, 3, 4, 5).verifyComplete();
    }

    @Test
    public void fluxSubscriberNumbersError() {
        var flux = Flux.range(1, 5)
                .map(i -> {
                    if (i == 4) {
                        throw new IndexOutOfBoundsException("index error");
                    }

                    return i;
                });

        flux.subscribe(
                i -> log.info("Number {}", i),
                Throwable::printStackTrace,
                () -> log.info("finished.")
        );

        StepVerifier.create(flux)
                .expectNext(1, 2, 3)
                .expectError(IndexOutOfBoundsException.class)
                .verify();
    }

    @Test
    public void fluxSubscriberNumbersBackpressure() {
        var flux = Flux.range(1, 5)
                .map(i -> {
                    if (i == 4) {
                        throw new IndexOutOfBoundsException("index error");
                    }

                    return i;
                });

        flux.subscribe(
                i -> log.info("Number {}", i),
                Throwable::printStackTrace,
                () -> log.info("finished."),
                subscription -> subscription.request(3)
        );

        StepVerifier.create(flux)
                .expectNext(1, 2, 3)
                .expectError(IndexOutOfBoundsException.class)
                .verify();
    }

    @Test
    public void fluxSubscriberNumbersUglyBackpressure() {
        var flux = Flux.range(1, 10).log();

        flux.subscribe(
                new Subscriber<Integer>() {
                    private int count = 0;
                    private final int requestCount = 2;
                    private Subscription subscription;

                    @Override
                    public void onSubscribe(Subscription s) {
                        this.subscription = s;
                        subscription.request(requestCount);
                    }

                    @Override
                    public void onNext(Integer integer) {
                        count++;
                        if (count >= 2) {
                            count = 0;
                            subscription.request(requestCount);
                        }
                    }

                    @Override
                    public void onError(Throwable t) {

                    }

                    @Override
                    public void onComplete() {

                    }
                }
        );

        StepVerifier.create(flux)
                .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .verifyComplete();
    }

    @Test
    public void fluxSubscriberNumbersNotSoUglyBackpressure() {
        var flux = Flux.range(1, 10).log();

        flux.subscribe(
                new BaseSubscriber<>() {
                    private int count = 0;
                    private final int requestCount = 2;

                    @Override
                    protected void hookOnSubscribe(Subscription subscription) {
                        request(requestCount);
                    }

                    @Override
                    protected void hookOnNext(Integer value) {
                        count++;
                        if (count >= 2) {
                            count = 0;
                            request(requestCount);
                        }
                    }
                }
        );

        StepVerifier.create(flux)
                .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .verifyComplete();
    }

    @Test
    public void fluxSubscriberInterval1() throws InterruptedException {
        var flux = Flux.interval(Duration.ofMillis(100)).take(10).log();

        flux.subscribe(
                i -> log.info("Number {}", i)
        );

        Thread.sleep(Duration.ofSeconds(3).toMillis());
    }

    @Test
    public void fluxSubscriberInterval2() {
        StepVerifier.withVirtualTime(this::createInterval)
                .expectSubscription()
                .expectNoEvent(Duration.ofHours(24))
                .thenAwait(Duration.ofDays(2))
                .expectNext(0L)
                .expectNext(1L)
                .thenCancel()
                .verify();
    }

    private Flux<Long> createInterval() {
        return Flux.interval(Duration.ofDays(1)).take(10).log();
    }
}
