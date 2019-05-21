package org.apache.flink.graph.streaming.example.test;

import com.google.common.collect.Lists;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.GraphStream;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.graph.streaming.library.BipartitenessCheck;
import org.apache.flink.graph.streaming.summaries.Candidates;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.NullValue;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class BipartitenessCheckTest extends AbstractTestBase {

		@Test
		public void testBipartite() throws Exception {

			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(1); //needed to ensure total ordering for windows
			CollectSink.values.clear();

			DataStream<Edge<Long, NullValue>> edges = env.fromCollection(getBipartiteEdges());
			GraphStream<Long, NullValue, NullValue> graph = new SimpleEdgeStream<>(edges, env);

			graph
				.aggregate(new BipartitenessCheck<>((long) 500))
				.addSink(new CollectSink());

			env.execute("Bipartiteness check");

			// verify the results
			assertEquals(Lists.newArrayList(
					"(true,{1={1=(1,true), 2=(2,false), 3=(3,false), 4=(4,false), 5=(5,true), 7=(7,true), 9=(9,true)}})"),
					CollectSink.values);

	}

	@Test
	public void testNonBipartite() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		CollectSink.values.clear();


		DataStream<Edge<Long, NullValue>> edges = env.fromCollection(getNonBipartiteEdges());
		GraphStream<Long, NullValue, NullValue> graph = new SimpleEdgeStream<>(edges, env);
		graph.
				aggregate(new BipartitenessCheck<>((long) 500))
				.addSink(new CollectSink());

		env.execute("Non Bipartiteness check");

		// verify the results
		assertEquals(Lists.newArrayList(
				"(false,{})"),
				CollectSink.values);

	}



	static List<Edge<Long, NullValue>> getBipartiteEdges () {
		List<Edge<Long, NullValue>> edges = new ArrayList<>();
		edges.add(new Edge<>(1L, 2L, NullValue.getInstance()));
		edges.add(new Edge<>(1L, 3L, NullValue.getInstance()));
		edges.add(new Edge<>(1L, 4L, NullValue.getInstance()));
		edges.add(new Edge<>(4L, 5L, NullValue.getInstance()));
		edges.add(new Edge<>(4L, 7L, NullValue.getInstance()));
		edges.add(new Edge<>(4L, 9L, NullValue.getInstance()));
		return edges;
	}

	static List<Edge<Long, NullValue>> getNonBipartiteEdges () {
		List<Edge<Long, NullValue>> edges = new ArrayList<>();
		edges.add(new Edge<>(1L, 2L, NullValue.getInstance()));
		edges.add(new Edge<>(2L, 3L, NullValue.getInstance()));
		edges.add(new Edge<>(3L, 1L, NullValue.getInstance()));
		edges.add(new Edge<>(4L, 5L, NullValue.getInstance()));
		edges.add(new Edge<>(5L, 7L, NullValue.getInstance()));
		edges.add(new Edge<>(4L, 1L, NullValue.getInstance()));
		return edges;
	}

	// a testing sink
	public static final class CollectSink implements SinkFunction<Candidates> {

		static final List<String> values = new ArrayList<>();

		@Override
		public void invoke(Candidates value, Context context) throws Exception {
			values.add(value.toString());
		}
	}
}