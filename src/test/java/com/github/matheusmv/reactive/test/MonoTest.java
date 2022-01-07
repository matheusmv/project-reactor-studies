package com.github.matheusmv.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
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
}
