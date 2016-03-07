package org.apache.flink.graph.streaming.example.test;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.GraphStream;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.graph.streaming.example.util.DisjointSet;
import org.apache.flink.graph.streaming.library.ConnectedComponents;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.StreamingProgramTestBase;
import org.apache.flink.types.NullValue;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ConnectedComponentsTest extends StreamingProgramTestBase {
	public static final String Connected_RESULT =
			"1, 2, 3, 5\n" + "6, 7\n" +
					"9 8\n";
	protected String resultPath;

	@SuppressWarnings("serial")
	private static DataStream<Edge<Long, NullValue>> getGraphStream(StreamExecutionEnvironment env) {
		return env.fromCollection(getEdges());
	}

	public static final List<Edge<Long, NullValue>> getEdges() {
		List<Edge<Long, NullValue>> edges = new ArrayList<>();
		edges.add(new Edge<>(1L, 2L, NullValue.getInstance()));
		edges.add(new Edge<>(1L, 3L, NullValue.getInstance()));
		edges.add(new Edge<>(2L, 3L, NullValue.getInstance()));
		edges.add(new Edge<>(1L, 5L, NullValue.getInstance()));
		edges.add(new Edge<>(6L, 7L, NullValue.getInstance()));
		edges.add(new Edge<>(8L, 9L, NullValue.getInstance()));
		return edges;
	}

	public static final String[] parser(ArrayList<String> list) {
		int s = list.size();
		String r = list.get(s - 1);  // to get the final combine result which is stored at the end of result
		String t = null;
		list.clear();
		String[] G = r.split("=");
		for (int i = 0; i < G.length; i++) {
			if (G[i].contains("[")) {
				String[] k = G[i].split("]");
				t = k[0].substring(1, k[0].length());
				list.add(t);
			}
		}
		String[] result = list.toArray(new String[list.size()]);
		Arrays.sort(result);
		return result;
	}

	@Override
	protected void preSubmit() throws Exception {
		setParallelism(1); //needed to ensure total ordering for windows
		resultPath = getTempDirPath("output");
	}

	@Override
	protected void postSubmit() throws Exception {
		String expectedResultStr = Connected_RESULT;
		String[] excludePrefixes = new String[0];
		ArrayList<String> list = new ArrayList<String>();
		readAllResultLines(list, resultPath, excludePrefixes, false);
		String[] result = parser(list);
		String[] expected = expectedResultStr.isEmpty() ? new String[0] : expectedResultStr.split("\n");
		Arrays.sort(expected);
		Assert.assertEquals("Different number of lines in expected and obtained result.", expected.length, result.length);
	}

	@Override
	protected void testProgram() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Edge<Long, NullValue>> edges = getGraphStream(env);
		GraphStream<Long, NullValue, NullValue> graph = new SimpleEdgeStream<>(edges, env);
		DataStream<DisjointSet<Long>> cc = graph.aggregate(new ConnectedComponents<Long, NullValue>(5));
		cc.writeAsText(resultPath);
		env.execute("Streaming Connected ComponentsCheck");
	}
}

