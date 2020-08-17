package org.apache.flink.training.assignments.functions;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.training.assignments.domain.Price;
import org.apache.flink.training.assignments.orders.DataStreamTest;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class PriceEnrichmentByAct extends RichCoFlatMapFunction<Position, Price, Position> {
    private static final Logger LOG = LoggerFactory.getLogger(PriceEnrichmentByAct.class);

    // keyed, managed state
    private ValueState<Position> positionState;
    private ValueState<Price> priceState;

    @Override
    public void open(Configuration config) {
        positionState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved Position", Position.class));
        priceState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved Price", Price.class));
    }

    @Override
    public void flatMap1(Position position, Collector<Position> out) throws Exception {
        //LOG.info("****flatMap1 {}", ++flatMap1);
        Price price = priceState.value();
        if (price != null) {
            //priceState.clear();
            out.collect(enrichPositionByActWithPrice(position,price.getPrice().doubleValue()));
        } else {
            positionState.update(position);
        }
    }

    @Override
    public void flatMap2(Price price, Collector<Position> out) throws Exception {
        //LOG.info("****flatMap2 {}", ++flatMap2);
        Position position = positionState.value();
        priceState.update(price);
        if (position != null) {
            positionState.clear();
            out.collect(enrichPositionByActWithPrice(position,price.getPrice().doubleValue()));
        }/**
        else {
            priceState.update(price);
        }**/
    }

    private Position enrichPositionByActWithPrice(final Position position, final double price) {

        position.setPrice(price);
        position.setMarketValue(position.getQuantity() * price);
        position.setTimestamp(System.currentTimeMillis());
        return position;
    }
}