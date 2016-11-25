package org.apache.flink.graph.streaming.example;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.util.TriangleEstimate;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The broadcast triangle count example estimates the number of triangles
 * in a streamed graph. The output is in <edges, triangles> format, where
 * edges refers to the number of edges processed so far.
 */
public class BroadcastTriangleCount implements ProgramDescription {

	public static void main(String[] args) throws Exception {

		// Set up the environment
		if(!parseParameters(args)) {
			return;
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Edge<Long, NullValue>> edges = getEdgesDataSet(env);

		int localSamples = samples / env.getParallelism();

		// Count triangles
		DataStream<Tuple2<Integer, Integer>> triangles = edges
				.broadcast()
				.flatMap(new TriangleSampler(localSamples, vertexCount))
				.flatMap(new TriangleSummer(samples, vertexCount))
				.setParallelism(1);

		// Emit the results
		if (fileOutput) {
			triangles.writeAsCsv(outputPath);
		} else {
			triangles.print();
		}

		env.execute("Broadcast Triangle Count");
	}

	// *************************************************************************
	//     TRIANGLE COUNT FUNCTIONS
	// *************************************************************************

	@SuppressWarnings("serial")
	private static final class TriangleSampler extends RichFlatMapFunction<Edge<Long, NullValue>, TriangleEstimate> {
		private List<SampleTriangleState> states;
		private int edgeCount;
		private int previousResult;
		private int vertices;

		public TriangleSampler(int size, int vertices) {
			this.states = new ArrayList<>();
			this.edgeCount = 0;
			this.previousResult = 0;
			this.vertices = vertices;

			for (int i = 0; i < size; ++i) {
				states.add(new SampleTriangleState());
			}
		}

		@Override
		public void flatMap(Edge<Long, NullValue> edge, Collector<TriangleEstimate> out) throws Exception {
			// Update edge count
			edgeCount++;

			int localBetaSum = 0;

			// Process the edge for all instances
			for (SampleTriangleState state : states) {

				// Flip a coin and with probability 1/i sample a candidate
				if (Coin.flip(state)) {
					state.srcVertex = edge.getSource();
					state.trgVertex = edge.getTarget();

					// Randomly sample the third vertex from V \ {src, trg}
					while (true) {
						state.thirdVertex = (int) Math.floor(Math.random() * vertices);

						if (state.thirdVertex != state.srcVertex && state.thirdVertex != state.trgVertex) {
							break;
						}
					}

					state.srcEdgeFound = false;
					state.trgEdgeFound = false;
					state.beta = 0;
				}

				if (state.beta == 0) {
					// Check if any of the two remaining edges in the candidate has been found
					if ((edge.getSource() == state.srcVertex && edge.getTarget() == state.thirdVertex)
							|| (edge.getSource() == state.thirdVertex && edge.getTarget() == state.srcVertex)) {
						state.srcEdgeFound = true;
					}

					if ((edge.getSource() == state.trgVertex && edge.getTarget() == state.thirdVertex)
							|| (edge.getSource() == state.thirdVertex && edge.getTarget() == state.trgVertex)) {
						state.trgEdgeFound = true;
					}

					state.beta = (state.srcEdgeFound && state.trgEdgeFound) ? 1 : 0;
				}

				if (state.beta == 1) {
					localBetaSum++;
				}
			}

			if (localBetaSum != previousResult) {
				previousResult = localBetaSum;

				int source = getRuntimeContext().getIndexOfThisSubtask();
				out.collect(new TriangleEstimate(source, edgeCount, localBetaSum));
			}
		}
	}

	@SuppressWarnings("serial")
	private static final class TriangleSummer
			implements FlatMapFunction<TriangleEstimate, Tuple2<Integer, Integer>> {
		private Map<Integer, TriangleEstimate> results;
		private int maxEdges;
		private int sampleSize;
		private int previousResult;
		private int vertices;

		public TriangleSummer(int sampleSize, int vertices) {
			this.results = new HashMap<>();
			this.maxEdges = 0;
			this.sampleSize = sampleSize;
			this.previousResult = 0;
			this.vertices = vertices;
		}

		@Override
		public void flatMap(TriangleEstimate estimate, Collector<Tuple2<Integer, Integer>> out) throws Exception {
			results.put(estimate.getSource(), estimate);

			if (estimate.getEdgeCount() > maxEdges) {
				maxEdges = estimate.getEdgeCount();
			}

			int globalBetaSum = 0;
			for (TriangleEstimate entry : results.values()) {
				globalBetaSum += entry.getBeta();
			}

			int result = (int) ((1.0 / (double) sampleSize) * globalBetaSum * maxEdges * (vertices - 2));
			if (result != previousResult) {
				previousResult = result;
				out.collect(new Tuple2<>(maxEdges, result));
			}

		}
	}

	@SuppressWarnings("serial")
	private static final class SampleTriangleState implements Serializable {
		public long beta;

		public long srcVertex;
		public long trgVertex;
		public long thirdVertex;

		public boolean srcEdgeFound;
		public boolean trgEdgeFound;

		public int i;

		public SampleTriangleState() {
			this.beta = 0L;

			this.thirdVertex = -1L;
			this.srcEdgeFound = false;
			this.trgEdgeFound = false;

			i = 1;
		}
	}

	private static final class Coin {
		public static boolean flip(SampleTriangleState state) {
			boolean result = (Math.random() * (state.i) < 1);
			state.i++;

			return result;
		}
	}

	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static String outputPath = null;
	private static int vertexCount = 1000;
	private static int samples = 10000;

	private static boolean parseParameters(String[] args) {

		if(args.length > 0) {
			if(args.length != 4) {
				System.err.println("Usage: BroadcastTriangleCount <input edges path> <output path> <vertex count> <sample count>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];
			vertexCount = Integer.parseInt(args[2]);
			samples = Integer.parseInt(args[3]);
		} else {
			System.out.println("Executing BroadcastTriangleCount example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("  Usage: BroadcastTriangleCount <input edges path> <output path> <vertex count> <sample count>");
		}
		return true;
	}

	@SuppressWarnings("serial")
	private static DataStream<Edge<Long, NullValue>> getEdgesDataSet(StreamExecutionEnvironment env) {

		if (fileOutput) {
			return env.readTextFile(edgeInputPath)
					.map(new MapFunction<String, Edge<Long, NullValue>>() {
						@Override
						public Edge<Long, NullValue> map(String s) throws Exception {
							String[] fields = s.split("\\t");
							long src = Long.parseLong(fields[0]);
							long trg = Long.parseLong(fields[1]);
							return new Edge<>(src, trg, NullValue.getInstance());
						}
					});
		}

		return env.generateSequence(0, 999).flatMap(
				new FlatMapFunction<Long, Edge<Long, NullValue>>() {
					@Override
					public void flatMap(Long key, Collector<Edge<Long, NullValue>> out) throws Exception {
						out.collect(new Edge<>(key, (key + 2) % 1000, NullValue.getInstance()));
						out.collect(new Edge<>(key, (key + 4) % 1000, NullValue.getInstance()));
					}
				});
	}

	@Override
	public String getDescription() {
		return "Broadcast Triangle Count";
	}
}