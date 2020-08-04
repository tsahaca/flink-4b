package org.apache.flink.training.assignments.watermarks;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.training.assignments.domain.Order;

public class PunctuatedWatermarkAssigner implements AssignerWithPunctuatedWatermarks<Order> {

    @Override
    public long extractTimestamp(Order element, long previousElementTimestamp) {
        return element.getTimestamp();
    }

    @Override
    public Watermark checkAndGetNextWatermark(Order lastElement, long extractedTimestamp) {
        /**
         * Custom business Logic can be added here
         */
        return new Watermark(extractedTimestamp);
    }
}
