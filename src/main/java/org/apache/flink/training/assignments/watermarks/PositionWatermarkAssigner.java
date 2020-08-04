package org.apache.flink.training.assignments.watermarks;


import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.training.assignments.domain.Order;
import org.apache.flink.training.assignments.domain.Position;


public class PositionWatermarkAssigner extends AscendingTimestampExtractor<Position> {

    @Override
    public long extractAscendingTimestamp(Position position) {
        return position.getTimestamp();
    }
}
