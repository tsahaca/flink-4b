package org.apache.flink.training.assignments.serializers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.training.assignments.domain.Order;

public class OrderKeyedSerializationSchema implements KeyedSerializationSchema<Tuple2<String, Order>> {
    static ObjectMapper objectMapper = new ObjectMapper();//.registerModule(new JavaTimeModule());

    private String topic;

    public OrderKeyedSerializationSchema(final String topic){
        this.topic=topic;
    }
    @Override
    public byte[] serializeKey(Tuple2<String, Order> element) {
        return element.f0.getBytes();
    }

    @Override
    public byte[] serializeValue(Tuple2<String, Order> element) {
        try {
            return objectMapper.writeValueAsBytes(element.f1);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public String getTargetTopic(Tuple2<String, Order> stringOrderTuple2) {
        return this.topic;
    }
}
