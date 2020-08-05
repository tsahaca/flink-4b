package org.apache.flink.training.assignments.domain;

import jdk.jfr.DataAmount;

import java.io.Serializable;
import java.math.BigDecimal;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Position extends IncomingEvent {

    private static final long serialVersionUID = -2499451017707868513L;
    private String account;
    private String subAccount;
    private String cusip;
    private int quantity;
    private double price;
    private double marketValue;

    @Override
    public byte[] key() {
        return account.getBytes();
    }

    public Position(final String act,
                    final String subAct,
                    final String cusip,
                    final int qty){
        this.account=act;
        this.subAccount=subAct;
        this.cusip=cusip;
        this.quantity=qty;

    }
}
