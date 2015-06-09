package org.apache.flink.graph.streaming.example.degrees;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.streaming.EdgeOnlyStream;
import org.apache.flink.graph.streaming.example.bipartiteness.StreamedBipartiteness;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.WindowMapFunction;
import org.apache.flink.streaming.api.windowing.helper.Count;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;

public class DegreeMeasurement {

	public static String inFile = "d:\\_work\\gelly-streaming\\movielens_20m_sorted.txt";
	public static String outFile = "d:\\_work\\gelly-streaming\\movielens_tmp_sorted.txt";

	public static void measureControl(String resultFile, int size) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.getConfig().disableSysoutLogging();

		DataStream<Edge<Long, NullValue>> edgeStream = env
				.readTextFile(outFile)
				.map(new MapFunction<String, Edge<Long, NullValue>>() {
					@Override
					public Edge<Long, NullValue> map(String s) throws Exception {
						String[] args = s.split(",");
						long src = Long.parseLong(args[0]);
						long trg = Long.parseLong(args[1]) + 1000000;
						return new Edge<>(src, trg, NullValue.getInstance());
					}
				});

		EdgeOnlyStream<Long, NullValue> graph = new EdgeOnlyStream<>(edgeStream, env);
		graph.mapEdges(new MapFunction<Edge<Long,NullValue>, Edge<Long, NullValue>>() {
			@Override
			public Edge<Long, NullValue> map(Edge<Long, NullValue> edge) throws Exception {
				return edge;
			}
		});

		long timeResult = env.execute().getNetRuntime();

		FileWriter res = new FileWriter(resultFile, true);
		res.append(String.format("(%d, %d)\n", size, timeResult));
		res.close();
	}

	public static void measureDegrees(String resultFile, int size) throws Exception {

		// Create environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.getConfig().disableSysoutLogging();

		DataStream<Edge<Long, NullValue>> edgeStream = env
				.readTextFile(outFile)
				.map(new MapFunction<String, Edge<Long, NullValue>>() {
					@Override
					public Edge<Long, NullValue> map(String s) throws Exception {
						String[] args = s.split(",");
						long src = Long.parseLong(args[0]);
						long trg = Long.parseLong(args[1]) + 1000000;
						return new Edge<>(src, trg, NullValue.getInstance());
					}
				});

		EdgeOnlyStream<Long, NullValue> graph = new EdgeOnlyStream<>(edgeStream, env);
		graph.getDegrees();
		long timeResult = env.execute().getNetRuntime();

		FileWriter res = new FileWriter(resultFile, true);
		res.append(String.format("(%d, %d)\n", size, timeResult));
		res.close();
	}

	public static void measureSerial(String resultFile, int size) throws Exception {

		// Create environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.getConfig().disableSysoutLogging();
		env.setParallelism(1);

		DataStream<Edge<Long, NullValue>> edgeStream = env
				.readTextFile(outFile)
				.map(new MapFunction<String, Edge<Long, NullValue>>() {
					@Override
					public Edge<Long, NullValue> map(String s) throws Exception {
						String[] args = s.split(",");
						long src = Long.parseLong(args[0]);
						long trg = Long.parseLong(args[1]) + 1000000;
						return new Edge<>(src, trg, NullValue.getInstance());
					}
				});

		EdgeOnlyStream<Long, NullValue> graph = new EdgeOnlyStream<>(edgeStream, env);
		graph.getDegrees();
		long timeResult = env.execute().getNetRuntime();

		FileWriter res = new FileWriter(resultFile, true);
		res.append(String.format("(%d, %d)\n", size, timeResult));
		res.close();
	}

	public static void measureWindows(String resultFile, int size) throws Exception {

		// Create environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.getConfig().disableSysoutLogging();

		DataStream<Edge<Long, NullValue>> edgeStream = env
				.readTextFile(outFile)
				.map(new MapFunction<String, Edge<Long, NullValue>>() {
					@Override
					public Edge<Long, NullValue> map(String s) throws Exception {
						String[] args = s.split(",");
						long src = Long.parseLong(args[0]);
						long trg = Long.parseLong(args[1]) + 1000000;
						return new Edge<>(src, trg, NullValue.getInstance());
					}
				});

		EdgeOnlyStream<Long, NullValue> graph = new EdgeOnlyStream<>(edgeStream, env);

		graph.getEdges()
				.window(Count.of(10000))
				.mapWindow(new DegreeTypeSeparator<Long, NullValue>(true, true))
				.groupBy(0)
				.flatten()
				.map(new DegreeMapFunction<Long>());

		long timeResult = env.execute().getNetRuntime();

		FileWriter res = new FileWriter(resultFile, true);
		res.append(String.format("(%d, %d)\n", size, timeResult));
		res.close();
	}

	public static void main(String[] args) throws Exception {
		String controlResult = "d:\\_work\\gelly-streaming\\degree_control_results.csv";
		String degreeResult = "d:\\_work\\gelly-streaming\\degree_results.csv";
		String windowResult = "d:\\_work\\gelly-streaming\\degree_window_results.csv";
		String serialResult = "d:\\_work\\gelly-streaming\\degree_serial_results.csv";

		// Measure different sizes
		for (int size = 100000; size <= 20000000; size += 100000) {
			System.out.println("Processing " + size);

			// Create new data set;
			FileWriter fw = new FileWriter(outFile, false);

			try (BufferedReader br = new BufferedReader(new FileReader(inFile))) {
				String line;
				int count = 0;
				while (count < size && (line = br.readLine()) != null) {
					// process the line.
					fw.write(line);
					fw.write("\r\n");
					count++;
				}
			}

			fw.close();

			measureControl(controlResult, size);
			measureDegrees(degreeResult, size);
			measureWindows(windowResult, size);
			measureSerial(serialResult, size);
		}

	}

	private static final class DegreeTypeSeparator <K, EV>
			implements WindowMapFunction<Edge<K, EV>, Vertex<K, Long>> {
		private final boolean collectIn;
		private final boolean collectOut;

		public DegreeTypeSeparator(boolean collectIn, boolean collectOut) {
			this.collectIn = collectIn;
			this.collectOut = collectOut;
		}

		@Override
		public void mapWindow(Iterable<Edge<K, EV>> edges, Collector<Vertex<K, Long>> out) throws Exception {
			for (Edge<K, EV> edge : edges) {
				if (collectOut) {
					out.collect(new Vertex<>(edge.getSource(), 1L));
				}
				if (collectIn) {
					out.collect(new Vertex<>(edge.getTarget(), 1L));
				}
			}
		}
	}

	private static final class DegreeMapFunction <K>
			implements MapFunction<Vertex<K, Long>, Vertex<K, Long>> {
		private final Map<K, Long> localDegrees;

		public DegreeMapFunction() {
			localDegrees = new HashMap<>();
		}

		@Override
		public Vertex<K, Long> map(Vertex<K, Long> degree) throws Exception {
			K key = degree.getId();
			if (!localDegrees.containsKey(key)) {
				localDegrees.put(key, 0L);
			}
			localDegrees.put(key, localDegrees.get(key) + degree.getValue());
			return new Vertex<>(key, localDegrees.get(key));
		}
	}
}
