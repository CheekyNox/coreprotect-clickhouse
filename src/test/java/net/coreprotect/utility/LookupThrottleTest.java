package net.coreprotect.utility;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.Test;

class LookupThrottleTest {

    @Test
    void concurrentLookupsForOnePlayerAcquireOnlyOnce() throws Exception {
        String player = "lookup-throttle-test-" + UUID.randomUUID();
        ExecutorService workers = Executors.newFixedThreadPool(12);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<Boolean>> attempts = new ArrayList<>();
            for (int index = 0; index < 12; index++) {
                attempts.add(workers.submit(() -> {
                    start.await();
                    return LookupThrottle.tryAcquire(player, 0);
                }));
            }
            start.countDown();

            int acquired = 0;
            for (Future<Boolean> attempt : attempts) {
                if (attempt.get()) {
                    acquired++;
                }
            }
            assertEquals(1, acquired);
        }
        finally {
            LookupThrottle.release(player);
            workers.shutdownNow();
        }
    }
}
