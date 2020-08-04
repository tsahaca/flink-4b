package org.apache.flink.training.assignments.keys;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.assignments.domain.Allocation;
import org.apache.flink.training.assignments.domain.BuySell;
import org.apache.flink.training.assignments.domain.Order;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SplitOrderWindowFunction extends ProcessAllWindowFunction<Order, Position, TimeWindow> {
    private static final Logger LOG = LoggerFactory.getLogger(SplitOrderWindowFunction.class);
    @Override
    public void process(Context context, Iterable<Order> iterable, Collector<Position> collector) throws Exception {
        //LOG.info("Flattening orders {}", iterable);
        for (Order order : iterable) {
            //LOG.debug("Getting accounts for orderId= {}", order.getOrderId());
            order.getAllocations().forEach(allocation -> {
                collector.collect(

                        new Position(allocation.getAccount(),
                                allocation.getSubAccount(),
                                order.getCusip(),
                                adjustQuantity(order,allocation)));
            });
        }

    }

    private  int adjustQuantity(final Order order, final Allocation allocation){
        return order.getBuySell()== BuySell.BUY ? allocation.getQuantity() : -allocation.getQuantity();
    }
}
