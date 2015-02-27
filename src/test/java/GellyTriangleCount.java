import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class GellyTriangleCount {

	public GellyTriangleCount() throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

		// TODO: Convert this to use arguments
		// TODO: Optimize the algorithm
		// TODO: Figure out why .where(0, 1) does not work

		// Get it from: http://snap.stanford.edu/data/twitter_combined.txt.gz
		// The result should be 13082506, according to http://snap.stanford.edu/data/egonets-Twitter.html
		DataSet<Edge<Long, NullValue>> edges = env.readTextFile("twitter_combined.txt")
				.flatMap(new FlatMapFunction<String, Edge<Long, NullValue>>() {
					@Override
					public void flatMap(String s, Collector<Edge<Long, NullValue>> out) throws Exception {

						// Parse lines from the text file
						String[] vertices = s.split(" ");
						long src = Long.parseLong(vertices[0]);
						long trg = Long.parseLong(vertices[1]);

						out.collect(new Edge<Long, NullValue>(src, trg, NullValue.getInstance()));
						out.collect(new Edge<Long, NullValue>(trg, src, NullValue.getInstance()));
					}
				}).distinct();

		DataSet<Vertex<Long, Long>> vertices = edges
				.flatMap(new FlatMapFunction<Edge<Long, NullValue>, Vertex<Long, Long>>() {
					@Override
					public void flatMap(Edge<Long, NullValue> edge, Collector<Vertex<Long, Long>> out)
							throws Exception {
						out.collect(new Vertex<Long, Long>(edge.getSource(), edge.getSource()));
					}
				})
				.distinct();

		Graph<Long, Long, NullValue> graph = Graph.fromDataSet(vertices, edges, env);

		DataSet<Tuple3<Long, Long, Long>> triads = graph
				.getEdges()
				.groupBy(0)
				.reduceGroup(new TriadGroupReducer());

		graph.getEdges()
				.join(triads)
				.where(new EdgeKeySelector()) // Note: .where(0, 1) fails for unknown reasons
				.equalTo(new TriadKeySelector()) // Note: .equalTo(1, 2) does not fail, however
				.map(new CountMapper()).sum(0).print();
				// .print();

		env.execute("Triangle count");
	}

	private static final class TriadGroupReducer
			implements GroupReduceFunction<Edge<Long, NullValue>, Tuple3<Long, Long, Long>> {

		@Override
		public void reduce(Iterable<Edge<Long, NullValue>> iterable,
				Collector<Tuple3<Long, Long, Long>> out) throws Exception {

			HashSet<Long> neighbors = new HashSet<Long>();
			Long sourceVertex = null;

			for (Edge<Long, NullValue> edge : iterable) {
				sourceVertex = edge.getSource();
				neighbors.add(edge.getTarget());
			}

			// Build triads of vertices and emit them
			// Constraint: sourceVertex < vertexA < vertexB
			for (long vertexA : neighbors) {
				if (sourceVertex >= vertexA) {
					continue;
				}

				for (long vertexB : neighbors) {
					if (vertexA >= vertexB) {
						continue;
					}

					out.collect(new Tuple3<Long, Long, Long>(sourceVertex, vertexA, vertexB));
				}
			}
		}
	}

	private static final class EdgeKeySelector implements KeySelector<Edge<Long, NullValue>, Tuple2<Long, Long>> {
		@Override
		public Tuple2<Long, Long> getKey(Edge<Long, NullValue> edge) throws Exception {
			return new Tuple2<Long, Long>(edge.getSource(), edge.getTarget());
		}
	}

	private static final class TriadKeySelector implements KeySelector<Tuple3<Long, Long, Long>, Tuple2<Long, Long>> {
		@Override
		public Tuple2<Long, Long> getKey(Tuple3<Long, Long, Long> triad) throws Exception {
			return new Tuple2<Long, Long>(triad.f1, triad.f2);
		}
	}

	private static final class CountMapper
			implements MapFunction<Tuple2<Edge<Long, NullValue>, Tuple3<Long, Long, Long>>, Tuple1<Long>> {
		@Override
		public Tuple1<Long> map(Tuple2<Edge<Long, NullValue>,
				Tuple3<Long, Long, Long>> input) throws Exception {
			return new Tuple1<Long>(1L);
		}
	}

	public static void main(String[] args) throws Exception {
		new GellyTriangleCount();
	}
}
