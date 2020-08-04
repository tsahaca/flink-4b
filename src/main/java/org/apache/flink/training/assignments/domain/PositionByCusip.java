package org.apache.flink.training.assignments.domain;


import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PositionByCusip extends IncomingEvent {
    private static final long serialVersionUID = 7946678872780554209L;
    private String cusip;
    private List<Allocation> allocations;

    public PositionByCusip(String cusip,  List<Allocation> allocations) {
        this.cusip = cusip;
        this.allocations = allocations;
    }

    public void addAllocation(final Allocation allocation){
        this.allocations.add(allocation);
    }

    public PositionByCusip() {
        this.allocations = new ArrayList<>();
    }

    public static PositionByCusip.PositionByCusipBuilder builder() {
        return new PositionByCusip.PositionByCusipBuilder();
    }

    public byte[] key() {
        return this.cusip.getBytes();
    }



    public String getCusip() {
        return this.cusip;
    }

    public void setCusip(String cusip) {
        this.cusip = cusip;
    }



    public List<Allocation> getAllocations() {
        return this.allocations;
    }

    public void setAllocations(List<Allocation> allocations) {
        this.allocations = allocations;
    }

    public String toString() {
        String var10000 = this.getCusip();
        return "PositionByCusip(cusip=" + var10000 + ", allocations=" + this.getAllocations() + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PositionByCusip order = (PositionByCusip) o;
        return
                Objects.equals(cusip, order.cusip) &&
                Objects.equals(allocations, order.allocations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cusip, allocations);
    }

    protected boolean canEqual(Object other) {
        return other instanceof PositionByCusip;
    }


    public static class PositionByCusipBuilder {

        private String cusip;

        private List<Allocation> allocations;

        PositionByCusipBuilder() {
        }



        public PositionByCusip.PositionByCusipBuilder cusip(String cusip) {
            this.cusip = cusip;
            return this;
        }

        public PositionByCusip.PositionByCusipBuilder allocations(List<Allocation> allocations) {
            this.allocations = allocations;
            return this;
        }

        public PositionByCusip build() {
            return new PositionByCusip(this.cusip, this.allocations);
        }

        public String toString() {
            return "PositionByCusip.PositionByCusipBuilder(cusip=" + this.cusip
                    + ", allocations=" + this.allocations + ")";
        }
    }
}

