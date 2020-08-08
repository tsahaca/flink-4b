package org.apache.flink.training.assignments.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.training.assignments.domain.PositionByCusip;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PositionByCusipDeserializationSchema implements KafkaDeserializationSchema<PositionByCusip>
{
    private static final Logger LOG = LoggerFactory.getLogger(PositionByCusipDeserializationSchema.class);

    static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public boolean isEndOfStream(PositionByCusip nextElement) {
        return false;
    }

    @Override
    public PositionByCusip deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        LOG.debug("*** deserializing Kafka ConsumerRecord with key={}", record.key());
        PositionByCusip position = objectMapper.readValue(record.value(), PositionByCusip.class);
        //position.setTimestamp(record.timestamp());
        LOG.debug("*** deserialized Kafka ConsumerRecord with key={}, positionByCusip={}", record.key(), position);
        return position;
    }

    @Override
    public TypeInformation<PositionByCusip> getProducedType() {
        return TypeInformation.of(PositionByCusip.class);
    }
}