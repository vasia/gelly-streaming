package org.apache.flink.graph.streaming.example.test;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.GraphStream;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.graph.streaming.example.util.Candidates;
import org.apache.flink.graph.streaming.library.BipartitenessCheck;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.StreamingProgramTestBase;
import org.apache.flink.types.NullValue;

import java.util.ArrayList;
import java.util.List;

public class NonBipartitnessCheckTest extends StreamingProgramTestBase {

	public static final String NonBipartite_RESULT =
			"(false,{})";
	protected String resultPath;

	private static DataStream<Edge<Long, NullValue>> getGraphStream(StreamExecutionEnvironment env) {
		return env.fromCollection(getEdges());
	}

	public static final List<Edge<Long, NullValue>> getEdges() {
		List<Edge<Long, NullValue>> edges = new ArrayList<>();
		edges.add(new Edge<>(1L, 2L, NullValue.getInstance()));
		edges.add(new Edge<>(2L, 3L, NullValue.getInstance()));
		edges.add(new Edge<>(3L, 1L, NullValue.getInstance()));
		edges.add(new Edge<>(4L, 5L, NullValue.getInstance()));
		edges.add(new Edge<>(5L, 7L, NullValue.getInstance()));
		edges.add(new Edge<>(4L, 1L, NullValue.getInstance()));
		return edges;
	}

	@Override
	protected void preSubmit() throws Exception {
		setParallelism(1); //needed to ensure total ordering for windows
		resultPath = getTempDirPath("output");
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(NonBipartite_RESULT, resultPath);
	}

	@Override
	protected void testProgram() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Edge<Long, NullValue>> edges = getGraphStream(env);
		GraphStream<Long, NullValue, NullValue> graph = new SimpleEdgeStream<>(edges, env);
		DataStream<Candidates> cc = graph.aggregate(new BipartitenessCheck<Long, NullValue>((long) 500));
		cc.writeAsText(resultPath);
		env.execute("Non Bipartiteness check");

	}
}
