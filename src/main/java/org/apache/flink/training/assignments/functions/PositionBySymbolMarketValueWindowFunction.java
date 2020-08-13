package org.apache.flink.training.assignments.functions;

import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.training.assignments.domain.PositionByCusip;
import org.apache.flink.util.Collector;

public class PositionBySymbolMarketValueWindowFunction implements AllWindowFunction<PositionByCusip, PositionByCusip,
        TimeWindow> {

    @Override
    public void apply(
            final TimeWindow timeWindow,
            final Iterable<PositionByCusip> positions,
            final Collector<PositionByCusip> collector
    ) throws Exception {

        //The main counting bit for position quantity
        for (PositionByCusip position : positions
        ) {
            collector.collect(position);
        }
    }
}
