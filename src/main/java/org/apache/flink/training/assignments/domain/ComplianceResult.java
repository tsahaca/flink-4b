package org.apache.flink.training.assignments.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class ComplianceResult extends IncomingEvent implements KeyedAware {

    private static final long serialVersionUID = -2967845551914885137L;

    private String orderId;
    private boolean success;

    private List<String> errors;

    @Override
    public byte[] key() {
        return orderId.getBytes();
    }

}