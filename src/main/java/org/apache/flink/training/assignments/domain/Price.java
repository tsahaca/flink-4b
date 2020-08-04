package org.apache.flink.training.assignments.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Price extends IncomingEvent{
    private String id;
    private String cusip;
    private BigDecimal price;
    private long effectiveDateTime;
    private long timestamp;
    private boolean eos;

    @Override
    public byte[] key() {
        return this.id.getBytes();
    }
}
