package org.apache.flink.training.assignments.functions;


import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.training.assignments.domain.Price;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


public class PositionPriceCoProcess extends CoProcessFunction<Position, Price, Position> {

    private static final Logger LOG = LoggerFactory.getLogger(PositionPriceCoProcess.class);


    //hold the most recent positions
    private MapState<String, Position> cachedPositionState;
    //hold the most recent price
    private ValueState<Price> priceState;
    //hold the most recent evaluation timestamp
    private ValueState<Long> lastEvaluationState;
    private ListState<Position> positionListState;


    @Override
    public void open(Configuration parameters) throws Exception {
        priceState = getRuntimeContext().getState(new ValueStateDescriptor<>("priceState", Price.class));
        lastEvaluationState = getRuntimeContext().getState(new ValueStateDescriptor<>("lastEvaluationState", Long.class));
        positionListState = getRuntimeContext().getListState((new ListStateDescriptor<>("saved Position List", Position.class)));


        MapStateDescriptor<String, Position> mdescription =
                new MapStateDescriptor<String, Position>("cachedPosition", TypeInformation.of(String.class),
                        TypeInformation.of(new TypeHint<Position>() {}));
        cachedPositionState = getRuntimeContext().getMapState(mdescription);

        //super.open(parameters);
    }

    @Override
    public void processElement1(Position position, Context context, Collector<Position> collector) throws Exception {
        /**
        final Price price = priceState.value();
        final Long timer = lastEvaluationState.value();
        if (price != null) {
            priceState.clear();
            if( timer != null) {
                context.timerService().deleteProcessingTimeTimer(timer);
            }

            collector.collect(enrichPositionByActWithPrice(position, price.getPrice().doubleValue()));
        }  else {
            positionListState.add(position);
        }*/
        String positionKey = position.getAccount() + position.getSubAccount() + position.getCusip();
        Position oldPos = cachedPositionState.get(positionKey);
        if( oldPos != null){
            position.setQuantity(position.getQuantity() + oldPos.getQuantity());
        }
        cachedPositionState.put(positionKey,position);


        // positionListState.add(position);


    }



    @Override
    public void processElement2(Price price, Context context, Collector<Position> collector) throws Exception {
        priceState.update(price);
        setupAlarm(context);
    }

    private void setupAlarm(Context context) throws Exception {
        if( null == lastEvaluationState.value()) {
            long currentTime = context.timerService().currentProcessingTime();
            long timeoutTime = currentTime + 60000;
            context.timerService().registerProcessingTimeTimer(timeoutTime);
            lastEvaluationState.update(timeoutTime);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Position> out) throws Exception {
            Timestamp timestamp2 = new Timestamp(timestamp);
            DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

            LOG.info("Timer Invoked at {}" , fmt.format(timestamp2.toLocalDateTime()));
            lastEvaluationState.clear();
            final Price price = priceState.value();
            //Iterable<Position> positionList=positionListState.get();
            Iterable<Position> positionList=cachedPositionState.values();

            if( (null != price) && (null != positionList) ){
                priceState.clear();
                cachedPositionState.clear();
                    for(Position pos: positionList){

                        out.collect(enrichPositionByActWithPrice(pos,price.getPrice().doubleValue()));
                        LOG.info("Calculating Mkt Value {}" , pos);
                    }
            }

    }
    private Position enrichPositionByActWithPrice(final Position position, final double price) {

        position.setPrice(price);
        position.setMarketValue(position.getQuantity() * price);
        position.setTimestamp(System.currentTimeMillis());
        return position;
    }

}
