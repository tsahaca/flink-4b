package org.apache.flink.training.assignments.keys;


import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.training.assignments.domain.Allocation;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.training.assignments.domain.PositionByCusip;

import java.util.ArrayList;
import java.util.List;

public class PositionAggregatorBySymbol
		implements AggregateFunction<Position, PositionByCusip, PositionByCusip> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8528772774907786176L;
	
	@Override
	public PositionByCusip createAccumulator() {
		return new PositionByCusip();
	}

	@Override
	public PositionByCusip add(Position value, PositionByCusip accumulator) {
		accumulator.setTimestamp(System.currentTimeMillis());
		accumulator.setCusip(value.getCusip());
		accumulator.addAllocation(new Allocation(value.getAccount(),
				value.getSubAccount(), value.getQuantity()));
		return accumulator;
	}

	@Override
	public PositionByCusip getResult(PositionByCusip accumulator) {
		return accumulator;
	}

	@Override
	public PositionByCusip merge(PositionByCusip a,
								 PositionByCusip b) {
		List<Allocation> list = new ArrayList<Allocation>();
		list.addAll(a.getAllocations());
		list.addAll(b.getAllocations());
		PositionByCusip pcusip=new PositionByCusip(a.getCusip(), list);
		pcusip.setTimestamp(System.currentTimeMillis());

		return pcusip;
	}

}
