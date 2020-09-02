package org.apache.flink.training.assignments.functions;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.assignments.domain.PositionByCusip;
import org.apache.flink.util.Collector;

public class PositionBySymbolMarketValueWindowFunction implements WindowFunction<PositionByCusip, PositionByCusip,
        String, TimeWindow> {

    @Override
    public void apply(final String key,
            final TimeWindow timeWindow,
            final Iterable<PositionByCusip> positions,
            final Collector<PositionByCusip> collector
    ) throws Exception {
        //The main counting bit for position quantity
        //int qty =0;

        PositionByCusip last=null;
        for (PositionByCusip position : positions

        ) {
            //qty += position.getQuantity();

            last=position;
        }
      //  last.setQuantity(qty);
        last.setMarketValue(last.getQuantity() * last.getPrice());
        last.setTimestamp(System.currentTimeMillis());
        collector.collect(last);
    }
}
