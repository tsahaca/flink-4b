package org.apache.flink.training.assignments.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class Price extends IncomingEvent {

    private static final long serialVersionUID = 1L;

    private String id;
    private String cusip;
    private BigDecimal price;
    private long effectiveDateTime;


    @Override
    public byte[] key() {
        return new byte[0];
    }
}
