package org.apache.flink.graph.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

public class Test {

	public Test() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

		env.generateSequence(0, 10).flatMap(new FlatMapFunction<Long, Tuple2<Long, Long>>() {
			@Override
			public void flatMap(Long v, Collector<Tuple2<Long, Long>> out) throws Exception {
				out.collect(new Tuple2<Long, Long>(v, v));
				out.collect(new Tuple2<Long, Long>(v, v));
			}
		}).groupBy(0).map(new RichMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>() {
			@Override
			public Tuple2<Long, Long> map(Tuple2<Long, Long> t) throws Exception {
				System.out.println(getRuntimeContext().getIndexOfThisSubtask() + " >> " + t);
				return t;
			}
		}).print();

		env.execute();
	}

	public static void main(String[] args) throws Exception {
		new Test();
	}
}
