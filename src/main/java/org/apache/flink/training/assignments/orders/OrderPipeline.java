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
        env.getConfig().setAutoWatermarkInterval(10000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //env.setParallelism(ExerciseBase.parallelism);

        /**
         * Create the Price Stream from Kafka and keyBy cusip
         */
         /**
        var priceStream = env.addSource(readFromKafka(IN_PRICE,new PriceDeserializationSchema()))
                .name("kfkaPriceReader").uid("kfkaPriceReader")
                .keyBy(price -> price.getCusip());
         priceStream.addSink(new LogSink<>(LOG,
                LogSink.LoggerEnum.INFO, "**** priceStream {}"));
         */

        /**
        var positionsByAct = env.addSource(readFromKafka(IN_POSITION_ACT,new PositionDeserializationSchema()))
                .name("kfkaPositionsByActReader").uid("kfkaPositionsByActReader")
                .keyBy(position -> position.getCusip());
        positionsByAct.addSink(new LogSink<>(LOG,
                LogSink.LoggerEnum.INFO, "**** positionsByAct {}"));
         */

        var positionBySymbol = env.addSource(readFromKafka(IN_POSITION_SYMBOL,new PositionByCusipDeserializationSchema()))
                .name("kfkaPriceReader").uid("kfkaPriceReader")
                .keyBy(positionSymbol -> positionSymbol.getCusip());
        positionBySymbol.addSink(new LogSink<>(LOG,
                LogSink.LoggerEnum.INFO, "**** positionsBySymbol {}"));




        /**
        var positionsByCusip = aggregatePositionsByCusip(aggregatedPositionsByAccount);
        FlinkKafkaProducer010<Tuple2<String, List<Allocation>>> flinkKafkaProducerCusip = new FlinkKafkaProducer010<Tuple2<String, List<Allocation>>>(
                KAFKA_ADDRESS, OUT_CUSIP, new CusipKeyedSerializationSchema(OUT_CUSIP));
        positionsByCusip.addSink(flinkKafkaProducerCusip)
                .name("PublishPositionByCusipToKafka")
                .uid("PublishPositionByCusipToKafka");
         */



        // execute the transformation pipeline
        env.execute("kafkaPrice");
    }

    /**
     * Read Block Orders from Kafka
     * @return
     */
    private FlinkKafkaConsumer010<IncomingEvent> readFromKafka(final String topic,
                                                       final KafkaDeserializationSchema deserializationSchema){
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", KAFKA_ADDRESS);
        props.setProperty("group.id", KAFKA_GROUP);

        // Create tbe Kafka Consumer here
        // Added KafkaDeserializationSchema
        FlinkKafkaConsumer010<IncomingEvent> flinkKafkaConsumer = new FlinkKafkaConsumer010(topic,
                deserializationSchema, props);
        return flinkKafkaConsumer;
    }

    /**
     * Split Orders by Account, sub-account and cusip
     */
    private DataStream<Position> splitOrderStream(final DataStream<Order> orderStream) {
        DataStream<Position> splitOrderByAccountStream = orderStream
                .flatMap(new OrderFlatMap())
                /**
                .assignTimestampsAndWatermarks(new OrderPeriodicWatermarkAssigner())
                .name("TimestampWatermark").uid("TimestampWatermark")
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new SplitOrderWindowFunction())
                 */
                .name("splitOrderByAllocation")
                .uid("splitOrderByAllocation");
        return splitOrderByAccountStream;
    }

    /**
     * Create positions
     * @param splitOrderByAccountStream
     * @return
     */
    private DataStream<Position> createPositions(final DataStream<Position> splitOrderByAccountStream){
        /**
         * Group the order by account, sub-account and cusip
         */
        var groupOrderByAccountWindowedStream=splitOrderByAccountStream
                .assignTimestampsAndWatermarks(new PositionPeriodicWatermarkAssigner())
                .name("TimestampWatermark").uid("TimestampWatermark")
                .keyBy(new AccountPositionKeySelector())
                .timeWindow(Time.seconds(10))
                //.window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .sum("quantity")
                .name("AggregatePositionByActSubActCusip")
                .uid("AggregatePositionByActSubActCusip");

        /**
         * Aggregate the position by account,sub-account and cusip
         */
        /**
        var aggregatedPositionsByAccountStream = groupOrderByAccountWindowedStream
                .apply(new PositionAggregationWindowFunction())
                .name("AggregatePositionByActSubActCusip")
                .uid("AggregatePositionByActSubActCusip");

        return aggregatedPositionsByAccountStream;
         */
        return groupOrderByAccountWindowedStream;
    }

    private DataStream<Tuple2<String, List<Allocation>>> aggregatePositionsByCusip(DataStream<Position> aggregatedPositionsByAccount){
        var positionsByCusip = aggregatedPositionsByAccount
                .keyBy(position -> position.getCusip())
                .timeWindow(Time.seconds(10))
                //.apply(new PositionByCusipWindowFunction())
                .aggregate(new PositionAggregatorByCusip())
                .name("AggregatePositionByCusip")
                .uid("AggregatePositionByCusip");
        return positionsByCusip;
    }

    private DataStream<PositionByCusip> aggregatePositionsBySymbol(DataStream<Position> aggregatedPositionsByAccount){
        var positionsByCusip = aggregatedPositionsByAccount
                .keyBy(position -> position.getCusip())
                .timeWindow(Time.seconds(10))
                //.apply(new PositionByCusipWindowFunction())
                .aggregate(new PositionAggregatorBySymbol())
                .name("AggregatePositionBySymbol")
                .uid("AggregatePositionBySymbol");
        return positionsByCusip;
    }

}
