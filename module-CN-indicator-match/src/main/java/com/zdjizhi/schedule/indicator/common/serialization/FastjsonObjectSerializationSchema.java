package com.zdjizhi.schedule.indicator.common.serialization;

import com.alibaba.fastjson2.JSON;
import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * Serializes objects to JSON using the Fastjson library.
 *
 * @param <T> The type of elements to be serialized.
 */
public class FastjsonObjectSerializationSchema<T> implements SerializationSchema<T> {

    @Override
    public byte[] serialize(final T element) {
        return JSON.toJSONBytes(element);
    }
}
