package org.apache.flink.training.assignments.keys;


import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.training.assignments.domain.Allocation;
import org.apache.flink.training.assignments.domain.Position;

import java.util.ArrayList;
import java.util.List;

public class PositionAggregatorByCusip
		implements AggregateFunction<Position, Tuple2<String, List<Allocation>>, Tuple2<String, List<Allocation>>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8528772774907786176L;
	
	@Override
	public Tuple2<String, List<Allocation>> createAccumulator() {
		return new Tuple2<String, List<Allocation>>("", new ArrayList<Allocation>());
	}

	@Override
	public Tuple2<String, List<Allocation>> add(Position value, Tuple2<String, List<Allocation>> accumulator) {
		accumulator.f0 = value.getCusip();
		accumulator.f1.add(new Allocation(value.getAccount(),
				value.getSubAccount(), value.getQuantity()));
		return accumulator;
	}

	@Override
	public Tuple2<String, List<Allocation>> getResult(Tuple2<String, List<Allocation>> accumulator) {
		return accumulator;
	}

	@Override
	public Tuple2<String, List<Allocation>> merge(Tuple2<String, List<Allocation>> a,
												  Tuple2<String, List<Allocation>> b) {
		List<Allocation> list = new ArrayList<Allocation>();
		list.addAll(a.f1);
		list.addAll(b.f1);
		return new Tuple2<String, List<Allocation>>(a.f0, list);
	}

}
