package com.jaspercloud.redis.lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.serializer.RedisSerializer;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RedisLockImpl implements Lock {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private final String redisKey;
    private RedisLockSupport redisLockSupport;
    private RedisTemplate<String, Long> redisTemplate;
    private Executor executor;
    private AtomicBoolean ttlWatch = new AtomicBoolean(false);
    private Lock lock = new ReentrantLock();

    public RedisLockImpl(RedisLockSupport redisLockSupport, String redisKey) {
        this.redisLockSupport = redisLockSupport;
        this.redisKey = redisKey;
        this.redisTemplate = this.redisLockSupport.getRedisTemplate();
        this.executor = this.redisLockSupport.getExecutor();
    }

    @Override
    public void lock() {
        try {
            lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        for (; ; ) {
            boolean lock = tryLock(-1, TimeUnit.MILLISECONDS);
            if (lock) {
                return;
            }
        }
    }

    @Override
    public boolean tryLock() {
        for (; ; ) {
            try {
                boolean lock = tryLock(-1, TimeUnit.MILLISECONDS);
                return lock;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        long millis = unit.toMillis(time);
        boolean lock = tryLockMillis(millis);
        return lock;
    }

    private boolean tryLockMillis(long millis) throws InterruptedException {
        long start = System.currentTimeMillis();
        for (; ; ) {
            Long ttl = tryAcquire(millis);
            long end = System.currentTimeMillis();
            long diffTime = end - start;
            if (null == ttl) {
                return true;
            } else if (diffTime > millis) {
                return false;
            }
        }
    }

    private Long tryAcquire(long millis) {
        if (-1 == millis) {
            millis = redisLockSupport.getWatchDogTime();
        }
        Long ttl = innerTryAcquire(millis);
        if (null == ttl) {
            if (ttlWatch.compareAndSet(false, true)) {
                long finalMillis = millis;
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        while (ttlWatch.get()) {
                            try {
                                try {
                                    lock.lock();
                                    ValueOperations<String, Long> forValue = redisTemplate.opsForValue();
                                    long timeout = System.currentTimeMillis() + finalMillis;
                                    forValue.set(redisKey, timeout);
                                } finally {
                                    lock.unlock();
                                }
                            } catch (Exception e) {
                                logger.error(e.getMessage(), e);
                            }
                            try {
                                long time = finalMillis / 10;
                                waitTime(time);
                            } catch (Exception e) {
                                logger.error(e.getMessage());
                            }
                        }
                    }
                });
            }
        }
        return ttl;
    }

    private Long innerTryAcquire(long millis) {
        if (redisTemplate.execute(new SetNXCommand(millis))) {
            return null;
        }
        ValueOperations<String, Long> forValue = redisTemplate.opsForValue();
        long timeout = forValue.get(redisKey);
        long ttl = timeout - System.currentTimeMillis();
        if (ttl > 0) {
            return ttl;
        }
        long newValue = System.currentTimeMillis() + millis + 1;
        long oldValue = forValue.getAndSet(redisKey, newValue);
        if (oldValue < System.currentTimeMillis()) {
            return null;
        }
        ttl = oldValue - System.currentTimeMillis();
        return ttl;
    }

    private void waitTime(long time) throws InterruptedException {
        synchronized (redisKey) {
            redisKey.wait(time);
        }
    }

    @Override
    public void unlock() {
        try {
            lock.lock();
            ttlWatch.set(false);
            redisTemplate.delete(redisKey);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    private class SetNXCommand implements RedisCallback<Boolean> {

        private long millis;

        public SetNXCommand(long millis) {
            this.millis = millis;
        }

        @Override
        public Boolean doInRedis(RedisConnection connection) throws DataAccessException {
            RedisSerializer keySerializer = redisTemplate.getKeySerializer();
            RedisSerializer valueSerializer = redisTemplate.getValueSerializer();
            long timeout = System.currentTimeMillis() + millis;
            byte[] key = keySerializer.serialize(redisKey);
            byte[] value = valueSerializer.serialize(timeout);
            Boolean nx = connection.setNX(key, value);
            return nx;
        }
    }
}
