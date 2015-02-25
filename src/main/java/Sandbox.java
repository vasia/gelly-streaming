import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.WindowMapFunction;
import org.apache.flink.streaming.api.windowing.deltafunction.DeltaFunction;
import org.apache.flink.streaming.api.windowing.helper.Count;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.HashMap;

public class Sandbox {

	public Sandbox() throws Exception {

		final HashMap<Long, Vertex<Long, Long>> vertexMap = new HashMap<Long, Vertex<Long, Long>>();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setBufferTimeout(1000);

		DataStream<String> source = env.readTextFile("random_graph.txt");

		DataStream<Triplet<Long, Double, Long>> triplets =
				source.map(new MapFunction<String, Triplet<Long, Double, Long>>() {
					@Override
					public Triplet<Long, Double, Long> map(String s)
							throws Exception {
						String[] vertices = s.split(" ");
						long source = Long.parseLong(vertices[0]);
						long target = Long.parseLong(vertices[1]);
						long weight = Long.parseLong(vertices[2]);

						if (source == 1L) {
							return new Triplet<Long, Double, Long>(new Vertex<Long, Double>(source, 0.0),
									new Edge<Long, Long>(source, target, weight),
									new Vertex<Long, Double>(target, Double.POSITIVE_INFINITY));
						}

						return new Triplet<Long, Double, Long>(new Vertex<Long, Double>(source, Double.POSITIVE_INFINITY),
								new Edge<Long, Long>(source, target, weight),
								new Vertex<Long, Double>(target, Double.POSITIVE_INFINITY));
					}
				});


		// Do shortest path inside a window
		WindowedDataStream<Triplet<Long, Double, Long>> tripletWindow = triplets.window(Count.of(1000));

		tripletWindow.groupBy("f2")
				.mapWindow(new WindowMapFunction<Triplet<Long,Double,Long>, Triplet<Long,Double,Long>>() {
					@Override
					public void mapWindow(Iterable<Triplet<Long, Double, Long>> iterable, Collector<Triplet<Long, Double, Long>> out) throws Exception {
						for (Triplet<Long,Double,Long> triplet : iterable) {
							out.collect(triplet);
						}
					}
				})
				.groupBy("f2")
				.flatten().print();

		env.execute("Sandbox");
	}

	public static class Triplet<K extends Comparable<K> & Serializable, VV extends Serializable,
			EV extends Serializable> extends Tuple3<Vertex<K, VV>, Edge<K, EV>, Vertex<K, VV>> {

		public Triplet() {}

		public Triplet(Vertex<K, VV> source, Edge<K, EV> edge, Vertex<K, VV> target) {
			super(source, edge, target);
		}
	}

	public static void main(String[] args) throws Exception {
		new Sandbox();
	}
}
