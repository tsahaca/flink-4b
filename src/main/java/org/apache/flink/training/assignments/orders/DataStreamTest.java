package org.apache.flink.training.assignments.orders;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.training.assignments.keys.AccountPositionKeySelector;
import org.apache.flink.training.assignments.keys.PositionAggregationWindowFunction;
import org.apache.flink.training.assignments.sinks.LogSink;
import org.apache.flink.training.assignments.utils.ExerciseBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
                                250
                        ),
                        new Position(
                                "AC1",
                                "SB1",
                                "CUSIP1",
                                -70
                        ),
                        new Position(
                                "AC2",
                                "SB2",
                                "CUSIP1",
                                300
                        ),
                        new Position(
                                "AC2",
                                "SB2",
                                "CUSIP1",
                                -250
                        ),
                        new Position(
                                "AC2",
                                "SB2",
                                "CUSIP1",
                                35
                        )
                ));
        positionDataStream.addSink(new LogSink<>(LOG,
                LogSink.LoggerEnum.INFO, "**** inputStream {}"));

        var output = positionDataStream.keyBy(new AccountPositionKeySelector())
                //.countWindow(3)
                //.window(TumblingProcessingTimeWindows.of(Time.milliseconds(10)))
                .timeWindow(Time.milliseconds(3))

                .sum("quantity");
                //.apply(new PositionAggregationWindowFunction());
        output.addSink(new LogSink<>(LOG,
                LogSink.LoggerEnum.INFO, "**** outputStream {}"));
        env.execute("DataStreamTest");


    }
}
