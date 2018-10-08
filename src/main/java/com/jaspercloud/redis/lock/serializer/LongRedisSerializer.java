package com.jaspercloud.redis.lock.serializer;

import org.apache.commons.lang3.math.NumberUtils;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;

import java.nio.charset.Charset;

public class LongRedisSerializer implements RedisSerializer<Long> {

    @Override
    public byte[] serialize(Long value) throws SerializationException {
        if (null == value) {
            return new byte[0];
        }
        return String.valueOf(value).getBytes(Charset.forName("utf-8"));
    }

    @Override
    public Long deserialize(byte[] bytes) throws SerializationException {
        if (null == bytes) {
            return 0L;
        }
        return NumberUtils.toLong(new String(bytes, Charset.forName("utf-8")), 0);
    }
}
