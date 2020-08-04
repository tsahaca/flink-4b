package org.apache.flink.training.assignments.serializers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.training.assignments.domain.Order;
import org.apache.kafka.clients.producer.ProducerRecord;
import javax.annotation.Nullable;

public class OrderKafkaSerializationSchema implements KafkaSerializationSchema<Tuple2<String,Order>> {
    static ObjectMapper objectMapper = new ObjectMapper();//.registerModule(new JavaTimeModule());
    private String topic;

    public OrderKafkaSerializationSchema(final String topic){
        this.topic=topic;
    }


    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple2<String,Order> element, @Nullable Long aLong) {
        try {
            final ProducerRecord<byte[],byte[]> producerRecord =
                    new ProducerRecord<byte[],byte[]>( this.topic, element.f0.getBytes(),
                            objectMapper.writeValueAsBytes(element.f1));
            return producerRecord;
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }
}
