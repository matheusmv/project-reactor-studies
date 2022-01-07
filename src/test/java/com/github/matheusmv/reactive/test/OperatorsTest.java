package com.github.matheusmv.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class OperatorsTest {

    @Test
    public void subscribeOnSimple() {
        // the events that are before and after subscribeOn will be executed in the same thread

        var flux = Flux.range(1, 4)
                .map(i -> {
                    log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .subscribeOn(Schedulers.single())
                .map(i -> {
                    log.info("Map 2 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4)
                .verifyComplete();
    }

    @Test
    public void publishOn() {
        // the events that are after publishOn will be executed in a different thread

        var flux = Flux.range(1, 4)
                .map(i -> {  // main thread
                    log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .publishOn(Schedulers.single())
                .map(i -> {
                    log.info("Map 2 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4)
                .verifyComplete();
    }

    @Test
    public void multipleSubscribeOn() {
        // only the first subscribeOn will be applied

        var flux = Flux.range(1, 4)
                .subscribeOn(Schedulers.single())
                .map(i -> {
                    log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .subscribeOn(Schedulers.boundedElastic())  // ignored
                .map(i -> {
                    log.info("Map 2 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4)
                .verifyComplete();
    }

    @Test
    public void multiplePublisherOn() {
        // the events that are after publishOn will be executed in a different thread

        var flux = Flux.range(1, 4)
                .publishOn(Schedulers.single())
                .map(i -> {
                    log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .publishOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("Map 2 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4)
                .verifyComplete();
    }

    @Test
    public void publisherOnAndSubscribeOn() {
        // publishOn has the highest order of precedence

        var flux = Flux.range(1, 4)
                .publishOn(Schedulers.single())
                .map(i -> {
                    log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .subscribeOn(Schedulers.boundedElastic())  // ignored
                .map(i -> {
                    log.info("Map 2 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4)
                .verifyComplete();
    }

    @Test
    public void subscribeOnAndPublisherOn() {
        // the events that are after publishOn will be executed in a different thread

        var flux = Flux.range(1, 4)
                .subscribeOn(Schedulers.single())
                .map(i -> {
                    log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .publishOn(Schedulers.boundedElastic())  // different thread
                .map(i -> {
                    log.info("Map 2 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4)
                .verifyComplete();
    }

    @Test
    public void subscribeOnIO() throws InterruptedException {
        // execute blocking operations or call external APIs in a separate thread
        // recommendation : Schedulers.boundedElastic()

        var listOfStrings = Mono.fromCallable(() -> Files.readAllLines(Path.of("text-file.txt")))
                .subscribeOn(Schedulers.boundedElastic());

        StepVerifier.create(listOfStrings)
                .expectSubscription()
                .thenConsumeWhile(list -> {
                    Assertions.assertFalse(list::isEmpty);
                    log.info("Size: {}", list.size());
                    return true;
                })
                .verifyComplete();
    }

    @Test
    public void switchIfEmptyOperator() {
        // when a publisher has nothing to return

        var flux = emptyFlux().switchIfEmpty(Flux.just("not empty anymore"));

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext("not empty anymore")
                .expectComplete();
    }

    private Flux<Object> emptyFlux() {
        return Flux.empty();
    }

    @Test
    public void deferOperator() throws InterruptedException {
        var mono = Mono.defer(() -> Mono.just(System.currentTimeMillis()));

        mono.subscribe(l -> log.info("time {}", l));
        Thread.sleep(200);

        mono.subscribe(l -> log.info("time {}", l));
        Thread.sleep(200);

        mono.subscribe(l -> log.info("time {}", l));
        Thread.sleep(200);

        mono.subscribe(l -> log.info("time {}", l));

        var atomicLong = new AtomicLong();
        mono.subscribe(atomicLong::set);
        Assertions.assertTrue(atomicLong.get() > 0);
    }

    @Test
    public void concatOperator() {
        var flux1 = Flux.just("A", "B");
        var flux2 = Flux.just("C", "D");
        var concatFlux = Flux.concat(flux1, flux2);

        StepVerifier.create(concatFlux)
                .expectSubscription()
                .expectNext("A", "B", "C", "D")
                .expectComplete()
                .verify();
    }

    @Test
    public void concatWithOperator() {
        var flux1 = Flux.just("A", "B");
        var flux2 = Flux.just("C", "D");
        var concatFlux = flux1.concatWith(flux2);

        StepVerifier.create(concatFlux)
                .expectSubscription()
                .expectNext("A", "B", "C", "D")
                .expectComplete()
                .verify();
    }

    @Test
    public void combineLatestOperator() {
        // delay can affect the result

        var flux1 = Flux.just("A", "B");
        var flux2 = Flux.just("C", "D");

        var combinedLatestFLux = Flux.combineLatest(
                flux1, flux2,
                (value1, value2) -> value1.toLowerCase().concat(value2.toLowerCase()));

        StepVerifier.create(combinedLatestFLux)
                .expectSubscription()
                .expectNext("bc", "bd")
                .expectComplete()
                .verify();
    }

    @Test
    public void mergeOperator() {
        var flux1 = Flux.just("A", "B").delayElements(Duration.ofMillis(2));
        var flux2 = Flux.just("C", "D");
        var mergeFlux = Flux.merge(flux1, flux2);

        StepVerifier.create(mergeFlux)
                .expectSubscription()
                .expectNext("C", "D")
                .expectNext("A", "B")
                .expectComplete()
                .verify();
    }

    @Test
    public void mergeWithOperator() {
        var flux1 = Flux.just("A", "B").delayElements(Duration.ofMillis(2));
        var flux2 = Flux.just("C", "D");
        var mergeFlux = flux1.mergeWith(flux2);

        StepVerifier.create(mergeFlux)
                .expectSubscription()
                .expectNext("C", "D")
                .expectNext("A", "B")
                .expectComplete()
                .verify();
    }

    @Test
    public void mergeSequentialOperator() {
        var flux1 = Flux.just("A", "B").delayElements(Duration.ofMillis(2));
        var flux2 = Flux.just("C", "D");
        var mergeFlux = Flux.mergeSequential(flux1, flux2, flux1);

        StepVerifier.create(mergeFlux)
                .expectSubscription()
                .expectNext("A", "B", "C", "D", "A", "B")
                .expectComplete()
                .verify();
    }

    @Test
    public void concatDelayErrorOperator() {
        var flux1 = Flux.just("A", "B").map(s -> {
            if ("B".equals(s)) {
                throw new IllegalArgumentException("illegal argument");
            }

            return s;
        });
        var flux2 = Flux.just("C", "D");
        var concatFlux = Flux.concatDelayError(flux1, flux2);

        StepVerifier.create(concatFlux)
                .expectSubscription()
                .expectNext("A", "C", "D")
                .expectError(IllegalArgumentException.class)
                .verify();
    }

    @Test
    public void mergeDelayErrorOperator() {
        var flux1 = Flux.just("A", "B").map(s -> {
            if ("B".equals(s)) {
                throw new IllegalArgumentException("illegal argument");
            }

            return s;
        });
        var flux2 = Flux.just("C", "D");
        var mergeFlux = Flux.mergeDelayError(1, flux1, flux2);

        StepVerifier.create(mergeFlux)
                .expectSubscription()
                .expectNext("A", "C", "D")
                .expectError(IllegalArgumentException.class)
                .verify();
    }
}
