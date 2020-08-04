package org.apache.flink.training.assignments.serializers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.training.assignments.domain.Allocation;
import org.apache.flink.training.assignments.domain.PositionByCusip;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SymbolKeyedSerializationSchema implements KeyedSerializationSchema<PositionByCusip> {
    private static final Logger LOG = LoggerFactory.getLogger(SymbolKeyedSerializationSchema.class);

    static ObjectMapper objectMapper = new ObjectMapper();//.registerModule(new JavaTimeModule());

    private String topic;

    public SymbolKeyedSerializationSchema(final String topic){
        this.topic=topic;
    }

    /**
     * set account number as the key of Kafa Record
     * @param element
     * @return
     */
    @Override
    public byte[] serializeKey(PositionByCusip element) {
        return element.getCusip().getBytes();
    }

    @Override
    public byte[] serializeValue(PositionByCusip element) {
        try {
            return objectMapper.writeValueAsBytes(element);
        } catch (JsonProcessingException e) {
            LOG.error("****ERROR in Serializing PositionByCusip {}", element);
        }
        return null;
    }

    @Override
    public String getTargetTopic(PositionByCusip element) {
        return this.topic;
    }
}
