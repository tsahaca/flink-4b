package org.apache.flink.training.assignments.keys;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.util.Collector;

public class PositionAggregationWindowFunction implements WindowFunction<Position, Position,
        Tuple3<String, String, String>, TimeWindow> {

    @Override
    public void apply(
            final Tuple3<String, String, String> tuple,
            final TimeWindow timeWindow,
            final Iterable<Position> positions,
            final Collector<Position> collector
    ) throws Exception {

        //The main counting bit for position quantity
        int count = 0;
        for (Position position : positions
        ) {
            count += position.getQuantity();
        }


        Position aggregatedPosition = new Position(tuple.f0,
                tuple.f1,
                tuple.f2,
                count);
        collector.collect(aggregatedPosition);
    }
}
