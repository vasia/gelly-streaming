package org.apache.flink.graph.streaming.example.test;

import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.GraphStream;
import org.apache.flink.graph.streaming.SimpleEdgeStream;
import org.apache.flink.graph.streaming.library.ConnectedComponents;
import org.apache.flink.graph.streaming.summaries.DisjointSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.NullValue;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ConnectedComponentsTest extends AbstractTestBase {

    @Test
    public void test() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //needed to ensure total ordering for windows
        CollectSink.values.clear();

        DataStream<Edge<Long, NullValue>> edges = getGraphStream(env);
        GraphStream<Long, NullValue, NullValue> graph = new SimpleEdgeStream<>(edges, env);

        graph
            .aggregate(new ConnectedComponents<>(5))
            .addSink(new CollectSink());

        env.execute("Streaming Connected Components Check");

        // verify the results
        String expectedResultStr = "1, 2, 3, 5\n" + "6, 7\n" + "8, 9\n";
        String[] result = parser(CollectSink.values);
        String[] expected = expectedResultStr.isEmpty() ? new String[0] : expectedResultStr.split("\n");

        assertEquals("Different number of lines in expected and obtained result.", expected.length, result.length);
        Assert.assertArrayEquals("Different connected components.", expected, result);
    }

	@SuppressWarnings("serial")
	private static DataStream<Edge<Long, NullValue>> getGraphStream(StreamExecutionEnvironment env) {
		return env.fromCollection(getEdges());
	}

	public static List<Edge<Long, NullValue>> getEdges() {
		List<Edge<Long, NullValue>> edges = new ArrayList<>();
		edges.add(new Edge<>(1L, 2L, NullValue.getInstance()));
		edges.add(new Edge<>(1L, 3L, NullValue.getInstance()));
		edges.add(new Edge<>(2L, 3L, NullValue.getInstance()));
		edges.add(new Edge<>(1L, 5L, NullValue.getInstance()));
		edges.add(new Edge<>(6L, 7L, NullValue.getInstance()));
		edges.add(new Edge<>(8L, 9L, NullValue.getInstance()));
		return edges;
	}

	static String[] parser(List<String> list) {
		int s = list.size();
		String r = list.get(s - 1);  // to get the final combine result which is stored at the end of result
		String t;
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

    // a testing sink
    public static final class CollectSink implements SinkFunction<DisjointSet<Long>> {

        static final List<String> values = new ArrayList<>();

        @Override
        public void invoke(DisjointSet value, Context context) throws Exception {
            values.add(value.toString());
        }
    }
}

