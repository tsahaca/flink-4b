package org.apache.flink.training.assignments.serializers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.training.assignments.domain.Allocation;
import org.apache.flink.training.assignments.domain.Position;

import java.util.List;

public class CusipKeyedSerializationSchema implements KeyedSerializationSchema<Tuple2<String, List<Allocation>>> {
    static ObjectMapper objectMapper = new ObjectMapper();//.registerModule(new JavaTimeModule());

    private String topic;

    public CusipKeyedSerializationSchema(final String topic){
        this.topic=topic;
    }

    /**
     * set account number as the key of Kafa Record
     * @param element
     * @return
     */
    @Override
    public byte[] serializeKey(Tuple2<String, List<Allocation>> element) {
        return element.f0.getBytes();
    }

    @Override
    public byte[] serializeValue(Tuple2<String, List<Allocation>> element) {
        try {
            return objectMapper.writeValueAsBytes(element);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public String getTargetTopic(Tuple2<String, List<Allocation>> element) {
        return this.topic;
    }
}
