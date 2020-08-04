package org.apache.flink.training.assignments.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PositionDeserializationSchema implements KafkaDeserializationSchema<Position>
{
    private static final Logger LOG = LoggerFactory.getLogger(PositionDeserializationSchema.class);

    static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public boolean isEndOfStream(Position nextElement) {
        return false;
    }

    @Override
    public Position deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        LOG.debug("*** deserializing Kafka ConsumerRecord with key={}", record.key());
        Position position = objectMapper.readValue(record.value(), Position.class);
        position.setTimestamp(record.timestamp());
        LOG.debug("*** deserialized Kafka ConsumerRecord with key={}, position={}", record.key(), position);
        return position;
    }

    @Override
    public TypeInformation<Position> getProducedType() {
        return TypeInformation.of(Position.class);
    }
}