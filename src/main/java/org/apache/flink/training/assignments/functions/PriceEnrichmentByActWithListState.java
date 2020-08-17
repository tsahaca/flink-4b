package org.apache.flink.training.assignments.functions;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.training.assignments.domain.Price;
import org.apache.flink.util.Collector;


public class PriceEnrichmentByActWithListState extends RichCoFlatMapFunction<Position, Price, Position> {
    // keyed, managed state
    private  ListState<Position> positionListState;
    private  ValueState<Price> priceState;

    @Override
    public void open(Configuration config) {
        priceState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved Price", Price.class));
        positionListState = getRuntimeContext().getListState((new ListStateDescriptor<>("saved Position List", Position.class)));
    }

    @Override
    public void flatMap1(Position position, Collector<Position> out) throws Exception {
        Price price = priceState.value();
        if (price != null) {
            //priceState.clear();
            out.collect(enrichPositionByActWithPrice(position,price.getPrice().doubleValue()));
        } else {
            positionListState.add(position);
        }
    }

    @Override
    public void flatMap2(Price price, Collector<Position> out) throws Exception {
        Iterable<Position> positionList=positionListState.get();
        //priceState.update(price);

        if(positionList != null){
            positionListState.clear();
            for(Position pos: positionList){
                out.collect(enrichPositionByActWithPrice(pos,price.getPrice().doubleValue()));
            }
        }
        else {
            priceState.update(price);
        }

    }

    private Position enrichPositionByActWithPrice(final Position position, final double price) {

        position.setPrice(price);
        position.setMarketValue(position.getQuantity() * price);
        position.setTimestamp(System.currentTimeMillis());
        return position;
    }
}