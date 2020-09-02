package org.apache.flink.training.assignments.functions;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.util.Collector;

public class PositionMarketValueWindowFunction implements  WindowFunction<Position, Position,
        Tuple3<String, String, String>, TimeWindow> {

    @Override
    public void apply(final Tuple3<String, String, String> key,
            final TimeWindow timeWindow,
            final Iterable<Position> positions,
            final Collector<Position> collector
    ) throws Exception {
        //The main counting bit for position quantity
       // int qty=0;
        Position lastPos=null;
        for (Position position : positions
        ) {
          //  qty += position.getQuantity();
            lastPos=position;
            //collector.collect(position);
        }
        //lastPos.setQuantity(qty);
        lastPos.setMarketValue(lastPos.getQuantity() * lastPos.getPrice());
        lastPos.setTimestamp(System.currentTimeMillis());
        collector.collect(lastPos);
    }
}
