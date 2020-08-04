package org.apache.flink.training.assignments.watermarks;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.training.assignments.domain.Order;

/**
 * This generator generates watermarks that are lagging behind processing time by a fixed amount.
 * It assumes that elements arrive in Flink after a bounded delay.
 */
public class TimeLagWatermarkAssigner implements AssignerWithPeriodicWatermarks<Order> {

    private final long maxTimeLag = 5000; // 5 seconds

    @Override
    public long extractTimestamp(Order element, long previousElementTimestamp) {
        return element.getTimestamp();
    }

    @Override
    public Watermark getCurrentWatermark() {
        // return the watermark as current time minus the maximum time lag
        return new Watermark(System.currentTimeMillis() - maxTimeLag);
    }
}