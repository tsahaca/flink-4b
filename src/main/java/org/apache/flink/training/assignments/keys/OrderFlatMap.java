package org.apache.flink.training.assignments.keys;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.training.assignments.domain.Allocation;
import org.apache.flink.training.assignments.domain.BuySell;
import org.apache.flink.training.assignments.domain.Order;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.util.Collector;

public class OrderFlatMap implements FlatMapFunction<Order, Position> {
    @Override
    public void flatMap(Order order, Collector<Position> collector) throws Exception {
        order.getAllocations().forEach(allocation -> {
            Position position=  new Position(allocation.getAccount(),
                    allocation.getSubAccount(),
                    order.getCusip(),
                    adjustQuantity(order,allocation));
            position.setTimestamp(order.getTimestamp());
            collector.collect(position);

        });
    }

    private  int adjustQuantity(final Order order, final Allocation allocation){
        return order.getBuySell()== BuySell.BUY ? allocation.getQuantity() : -allocation.getQuantity();
    }
}
