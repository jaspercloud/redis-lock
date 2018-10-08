package com.jaspercloud.redis;

import com.jaspercloud.redis.lock.RedisLockSupport;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.redis.connection.RedisConnectionFactory;

import java.util.Date;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

@SpringBootApplication
public class RedisLockApplication implements InitializingBean {

    public static void main(String[] args) throws InterruptedException {
        SpringApplication.run(RedisLockApplication.class, args);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await();
    }

    @Autowired
    private RedisConnectionFactory redisConnectionFactory;

    @Override
    public void afterPropertiesSet() {
        RedisLockSupport redisLockSupport = new RedisLockSupport(redisConnectionFactory);
        redisLockSupport.afterPropertiesSet();

        int threads = 64;
        ExecutorService executorService = Executors.newCachedThreadPool();
        CyclicBarrier cyclicBarrier = new CyclicBarrier(threads);
        CountDownLatch countDownLatch = new CountDownLatch(threads);
        AtomicInteger counter = new AtomicInteger(0);
        AtomicLong time = new AtomicLong(System.currentTimeMillis());
        for (int i = 0; i < threads; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        cyclicBarrier.await();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    Lock lock = redisLockSupport.newLock("redislock");
                    try {
                        long sleep = 100;
                        if (lock.tryLock(10, TimeUnit.MILLISECONDS)) {
                            long end = time.get();
                            long diffTime = System.currentTimeMillis() - end;
                            String format = DateFormatUtils.format(new Date(), "HH:mm:ss.SSS");
                            if (diffTime > sleep) {
                                int i = counter.incrementAndGet();
                                System.out.println(String.format("i=%d, status=%s, diffTime=%d, date=%s", i, true, diffTime, format));
                            } else {
                                int i = counter.incrementAndGet();
                                System.out.println(String.format("i=%d, status=%s, diffTime=%d, date=%s", i, false, diffTime, format));
                            }
                            time.set(System.currentTimeMillis());
                            Thread.sleep(sleep);
                        } else {
                            int i = counter.incrementAndGet();
                            System.out.println(String.format("i=%d, status=%s", i, false));
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        lock.unlock();
                    }
                    countDownLatch.countDown();
                }
            });
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("test");
    }
}
