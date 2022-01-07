package com.github.matheusmv.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

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
}
