package org.apache.flink.training.assignments.serializers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.training.assignments.domain.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderSerializationSchema
        implements SerializationSchema<Order> {
    private static final Logger LOG = LoggerFactory.getLogger(OrderSerializationSchema.class);

    private static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(Order element) {
        try {
            return objectMapper.writeValueAsBytes(element);
        } catch (JsonProcessingException e) {
           LOG.error("*** Failed to convert to JSON {}", element);
        }
        return null;
    }
}