package org.apache.flink.training.assignments.keys;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.assignments.domain.Allocation;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class PositionByCusipWindowFunction implements WindowFunction<Position, Tuple2<String,List<Allocation>>,
        String, TimeWindow> {

    @Override
    public void apply(
            final String cusip,
            final TimeWindow timeWindow,
            final Iterable<Position> positions,
            final Collector<Tuple2<String,List<Allocation>>> collector
    ) throws Exception {

        //The main counting bit for position quantity
        int count = 0;
        List<Allocation> allocations= new ArrayList<>();

        for (Position position : positions
        ) {
            allocations.add(new Allocation(position.getAccount(),
                    position.getSubAccount(), position.getQuantity()));
        }
        collector.collect(Tuple2.of(cusip,allocations));
    }
}
