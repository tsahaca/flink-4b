package org.apache.flink.training.assignments.functions;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.util.Collector;

public class PositionMarketValueWindowFunction implements  WindowFunction<Position, Position,
        String, TimeWindow> {

    @Override
    public void apply(final String key,
            final TimeWindow timeWindow,
            final Iterable<Position> positions,
            final Collector<Position> collector
    ) throws Exception {

        //The main counting bit for position quantity
        for (Position position : positions
        ) {
            collector.collect(position);
        }
    }
}
