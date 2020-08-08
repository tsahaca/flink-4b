package org.apache.flink.training.assignments.domain;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;



@JsonIgnoreProperties(ignoreUnknown = true)
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PositionByCusip extends IncomingEvent {
    private static final long serialVersionUID = 7946678872780554209L;
    private String cusip;
    private int quantity;
    private double price;
    private double marketValue;
    private String orderId;

    public PositionByCusip(final String csip,
                           final int qty){
        this.cusip=csip;
        this.quantity=qty;

    }

    @Override
    public byte[] key() {
        return cusip.getBytes();
    }
}




