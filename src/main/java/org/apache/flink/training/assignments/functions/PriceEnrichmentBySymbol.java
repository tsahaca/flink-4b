package org.apache.flink.training.assignments.functions;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.training.assignments.domain.Allocation;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.training.assignments.domain.PositionByCusip;
import org.apache.flink.training.assignments.domain.Price;
import org.apache.flink.util.Collector;


public class PriceEnrichmentBySymbol extends RichCoFlatMapFunction<PositionByCusip, Price, PositionByCusip> {
    // keyed, managed state
    private ValueState<PositionByCusip> positionState;
    private ValueState<Price> priceState;

    @Override
    public void open(Configuration config) {
        positionState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved PositionByCusip", PositionByCusip.class));
        priceState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved Price for PositionByCusip", Price.class));
    }

    @Override
    public void flatMap1(PositionByCusip position, Collector<PositionByCusip> out) throws Exception {
        Price price = priceState.value();
        if (price != null) {
            //priceState.clear();
            out.collect(enrichPositionBySymbolWithPrice(position,price.getPrice().doubleValue()));
        } else {
            positionState.update(position);
        }
    }

    @Override
    public void flatMap2(Price price, Collector<PositionByCusip> out) throws Exception {
        PositionByCusip position = positionState.value();
        if (position != null) {
            positionState.clear();
            out.collect(enrichPositionBySymbolWithPrice(position,price.getPrice().doubleValue()));
        } else {
            priceState.update(price);
        }
    }

    private PositionByCusip enrichPositionBySymbolWithPrice(final PositionByCusip position,
                                                  final double price){
        /**
        final PositionByCusip enrichedPos = new PositionByCusip(
                position.getCusip(),
                position.getQuantity(),
                price,
                position.getQuantity() * price, position.getOrderId());
        enrichedPos.setTimestamp(System.currentTimeMillis());
        return enrichedPos;
         */
        position.setPrice(price);
        position.setMarketValue(position.getQuantity() * price);
        position.setTimestamp(System.currentTimeMillis());
        return position;
    }
}