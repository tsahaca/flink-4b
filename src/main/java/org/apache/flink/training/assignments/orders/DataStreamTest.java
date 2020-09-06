package org.apache.flink.training.assignments.orders;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.training.assignments.domain.Price;
import org.apache.flink.training.assignments.functions.PriceEnrichmentByAct;
import org.apache.flink.training.assignments.functions.PriceEnrichmentByActWithListState;
import org.apache.flink.training.assignments.keys.AccountPositionKeySelector;
import org.apache.flink.training.assignments.keys.PositionAggregationWindowFunction;
import org.apache.flink.training.assignments.sinks.LogSink;
import org.apache.flink.training.assignments.utils.ExerciseBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Arrays;

public class DataStreamTest extends ExerciseBase {
    private static final Logger LOG = LoggerFactory.getLogger(DataStreamTest.class);

    public static void main(String[] args) throws Exception{
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(1);
        DataStream<Position> positionDataStream=
                env.fromCollection(Arrays.asList(
                        new Position(
                                "AC1",
                                "SB1",
                                "CUSIP1",
                                100
                        ),
                        new Position(
                                "AC1",
                                "SB1",
                                "CUSIP1",
                                200
                        ),
                        new Position(
                                "AC1",
                                "SB1",
                                "CUSIP1",
                                300
                        )
                        /**,
                        new Position(
                                "AC4",
                                "SB4",
                                "CUSIP2",
                                400
                        ),
                        new Position(
                                "AC5",
                                "SB5",
                                "CUSIP3",
                                500
                        )*/
                ))//.keyBy(position -> position.getCusip());
                .keyBy(new AccountPositionKeySelector())
                .sum("quantity")
                        .keyBy(position -> position.getCusip());

        DataStream<Price> priceDataStream=
                env.fromCollection(Arrays.asList(
                        new Price(
                                "1",
                                "CUSIP1",
                                new BigDecimal(5.0),
                                System.currentTimeMillis()
                        )
                        ,

                        new Price(
                                "2",
                                "CUSIP1",
                                new BigDecimal(15.0),
                                System.currentTimeMillis()
                        )
                        ,
                        new Price(
                                "3",
                                "CUSIP1",
                                new BigDecimal(25.0),
                                System.currentTimeMillis()
                        )

                )).keyBy(price -> price.getCusip());

        var priceEnrichedPositions= positionDataStream
                .connect(priceDataStream)
               // .flatMap(new PriceEnrichmentByActWithListState())
                .flatMap(new PriceEnrichmentByAct())
                .name("AccountPositionEnrichment")
                .uid("AccountPositionEnrichment");

        priceEnrichedPositions.addSink(new LogSink<>(LOG,
                LogSink.LoggerEnum.INFO, "**** outStream {}"));
        /**
        positionDataStream.addSink(new LogSink<>(LOG,
                LogSink.LoggerEnum.INFO, "**** Rolling Aggregated Position: {}"));*/


        env.execute("DataStreamTest");


    }
}
