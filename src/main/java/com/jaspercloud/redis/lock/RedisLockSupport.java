package com.jaspercloud.redis.lock;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;

public class RedisLockSupport implements InitializingBean {

    private RedisConnectionFactory connectionFactory;
    private RedisTemplate<String, Long> redisTemplate;
    private Executor executor;
    private long watchDogTime = 10 * 1000;

    public RedisTemplate<String, Long> getRedisTemplate() {
        return redisTemplate;
    }

    public Executor getExecutor() {
        return executor;
    }

    public long getWatchDogTime() {
        return watchDogTime;
    }

    public void setWatchDogTime(long watchDogTime) {
        this.watchDogTime = watchDogTime;
    }

    public RedisLockSupport(RedisConnectionFactory connectionFactory) {
        this(connectionFactory, Executors.newCachedThreadPool());
    }

    public RedisLockSupport(RedisConnectionFactory connectionFactory, Executor executor) {
        this.connectionFactory = connectionFactory;
        this.executor = executor;
    }

    @Override
    public void afterPropertiesSet() {
        redisTemplate = new RedisTemplate<>();
        redisTemplate.setKeySerializer(new StringRedisSerializer());
        redisTemplate.setValueSerializer(new LongRedisSerializer());
        redisTemplate.setConnectionFactory(connectionFactory);
        redisTemplate.afterPropertiesSet();
    }

    public Lock newLock(String redisKey) {
        return new RedisLockImpl(this, redisKey);
    }
}
