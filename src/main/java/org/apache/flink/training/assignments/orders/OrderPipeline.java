package org.apache.flink.training.assignments.orders;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.training.assignments.domain.*;
import org.apache.flink.training.assignments.functions.PriceEnrichmentByAct;
import org.apache.flink.training.assignments.functions.PriceEnrichmentBySymbol;
import org.apache.flink.training.assignments.keys.*;
import org.apache.flink.training.assignments.serializers.*;
import org.apache.flink.training.assignments.sinks.LogSink;
import org.apache.flink.training.assignments.watermarks.PositionPeriodicWatermarkAssigner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * The main class to process block orders received
 * from kafka and create positions by account, sub-account
 * cusip and publish to kafka
 */
public class OrderPipeline {
    private final String KAFKA_ADDRESS;
    private final String KAFKA_GROUP;

    private final String IN_PRICE;
    private final String IN_POSITION_ACT;
    private final String IN_POSITION_SYMBOL;

    private final String OUT_MV_ACT;
    private final String OUT_MV_SYMBOL;

    private static final Logger LOG = LoggerFactory.getLogger(OrderPipeline.class);

    public OrderPipeline(final Map<String,String> params){
        this.KAFKA_ADDRESS=params.get(IConstants.KAFKA_ADDRESS);
        this.KAFKA_GROUP=params.get(IConstants.KAFKA_GROUP);

        this.IN_PRICE=params.get(IConstants.IN_PRICE);
        this.IN_POSITION_ACT=params.get(IConstants.IN_POSITION_ACT);
        this.IN_POSITION_SYMBOL=params.get(IConstants.IN_POSITION_SYMBOL);

        this.OUT_MV_ACT=params.get(IConstants.OUT_MV_ACT);
        this.OUT_MV_SYMBOL=params.get(IConstants.OUT_MV_SYMBOL);

    }


    public void execute() throws Exception{
        // set up streaming execution environment
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
       // env.getConfig().setAutoWatermarkInterval(10000);
       // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //env.setParallelism(ExerciseBase.parallelism);

        /**
         * Create the Price Stream from Kafka and keyBy cusip
         */

        DataStream<Price> priceStream = env.addSource(readPriceFromKafka(IN_PRICE,new PriceDeserializationSchema()))
                .name("kfkaPriceReader").uid("kfkaPriceReader")
                .keyBy(price -> price.getCusip());
         //priceStream.addSink(new LogSink<>(LOG,
           //     LogSink.LoggerEnum.INFO, "**** priceStream {}"));



        DataStream<Position> positionsByAct = env.addSource(readPositionActFromKafka(IN_POSITION_ACT,new PositionDeserializationSchema()))
                .name("kfkaPositionsByActReader").uid("kfkaPositionsByActReader")
                .keyBy(position -> position.getCusip());
        //positionsByAct.addSink(new LogSink<>(LOG,
         //       LogSink.LoggerEnum.INFO, "**** positionsByAct {}"));

        var positionBySymbol = env.addSource(readPositionSymbolFromKafka(IN_POSITION_SYMBOL,new PositionByCusipDeserializationSchema()))
                .name("kfkaPositionsBySymbolReader").uid("kfkaPositionsBySymbolReader")
                .keyBy(positionSymbol -> positionSymbol.getCusip());
        //positionBySymbol.addSink(new LogSink<>(LOG,
               //LogSink.LoggerEnum.INFO, "**** positionsBySymbol {}"));

        var priceEnrichedPositions= positionsByAct
                .connect(priceStream)
                .flatMap(new PriceEnrichmentByAct())
                .uid("PriceEnrichedPositionsByAct");
        priceEnrichedPositions.addSink(new LogSink<>(LOG,
                LogSink.LoggerEnum.INFO, "**** priceEnrichedPositionsByAct {}"));

        /**
         * Publish the positions with Market Value By Act to kafka
         * set account number as the key of Kafa Record
         */
        FlinkKafkaProducer010<Position> flinkKafkaProducer = new FlinkKafkaProducer010<Position>(
                KAFKA_ADDRESS, OUT_MV_ACT, new PositionKeyedSerializationSchema(OUT_MV_ACT));
        priceEnrichedPositions.addSink(flinkKafkaProducer)
                .name("PublishPositionMarketValueByActToKafka")
                .uid("PublishPositionMarketValueByActToKafka");

        var priceEnrichedPositionsBySymbol= positionBySymbol
                .connect(priceStream)
                .flatMap(new PriceEnrichmentBySymbol())
                .uid("PriceEnrichedPositionsBySymbol");
        priceEnrichedPositionsBySymbol.addSink(new LogSink<>(LOG,
                LogSink.LoggerEnum.INFO, "**** priceEnrichedPositionsBySymbol {}"));

        /**
         * Publish the positions with Market Value By Symbol to kafka
         * set account number as the key of Kafa Record
         */
        FlinkKafkaProducer010<PositionByCusip> flinkKafkaProducerBySmbol = new FlinkKafkaProducer010<PositionByCusip>(
                KAFKA_ADDRESS, OUT_MV_SYMBOL, new SymbolKeyedSerializationSchema(OUT_MV_SYMBOL));
        priceEnrichedPositionsBySymbol.addSink(flinkKafkaProducerBySmbol)
                .name("PublishPositionMarketValueBySymbolToKafka")
                .uid("PublishPositionMarketValueBySymbolToKafka");



        // execute the transformation pipeline
        env.execute("kafkaPrice");
    }

    /**
     * Read Block Orders from Kafka
     * @return
     */
    private FlinkKafkaConsumer010<Price> readPriceFromKafka(final String topic,
                                                       final KafkaDeserializationSchema deserializationSchema){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", KAFKA_ADDRESS);
        props.setProperty("group.id", KAFKA_GROUP);

        // Create tbe Kafka Consumer here
        // Added KafkaDeserializationSchema
        FlinkKafkaConsumer010<Price> flinkKafkaConsumer = new FlinkKafkaConsumer010(topic,
                deserializationSchema, props);
        return flinkKafkaConsumer;
    }

    private FlinkKafkaConsumer010<Position> readPositionActFromKafka(final String topic,
                                                            final KafkaDeserializationSchema deserializationSchema){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", KAFKA_ADDRESS);
        props.setProperty("group.id", KAFKA_GROUP);

        // Create tbe Kafka Consumer here
        // Added KafkaDeserializationSchema
        FlinkKafkaConsumer010<Position> flinkKafkaConsumer = new FlinkKafkaConsumer010(topic,
                deserializationSchema, props);
        return flinkKafkaConsumer;
    }

    private FlinkKafkaConsumer010<PositionByCusip> readPositionSymbolFromKafka(final String topic,
                                                                  final KafkaDeserializationSchema deserializationSchema){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", KAFKA_ADDRESS);
        props.setProperty("group.id", KAFKA_GROUP);

        // Create tbe Kafka Consumer here
        // Added KafkaDeserializationSchema
        FlinkKafkaConsumer010<PositionByCusip> flinkKafkaConsumer = new FlinkKafkaConsumer010(topic,
                deserializationSchema, props);
        return flinkKafkaConsumer;
    }



}
