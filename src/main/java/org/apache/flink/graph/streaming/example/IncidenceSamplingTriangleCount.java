package org.apache.flink.graph.streaming.example;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.streaming.util.SampledEdge;
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
import java.util.Random;

public class IncidenceSamplingTriangleCount implements ProgramDescription {

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
				.flatMap(new EdgeSampleMapper(localSamples, env.getParallelism()))
				.setParallelism(1)
				.keyBy(0)
				.flatMap(new TriangleSampleMapper(localSamples, vertexCount))
				.flatMap(new TriangleSummer(samples, vertexCount))
				.setParallelism(1);

		// Emit the results
		if (fileOutput) {
			triangles.writeAsCsv(outputPath);
		} else {
			triangles.print();
		}

		env.execute("Incidence Sampling Triangle Count");
	}

	// *************************************************************************
	//     TRIANGLE COUNT FUNCTIONS
	// *************************************************************************

	@SuppressWarnings("serial")
	private static final class EdgeSampleMapper extends RichFlatMapFunction<Edge<Long, NullValue>, SampledEdge> {
		private final int instanceSize, p;
		private final List<Random> randoms;
		private final List<Edge<Long, NullValue>> samples;

		private int edgeCount;

		public EdgeSampleMapper(int instanceSize, int p) {
			this.instanceSize = instanceSize;
			this.p = p;

			this.edgeCount = 0;

			// Initialize seeds
			randoms = new ArrayList<>();
			samples = new ArrayList<>();

			Random r = new Random(0xDEADBEEF);
			for (int i = 0; i < instanceSize * p; ++i) {
				randoms.add(new Random(r.nextInt()));
				samples.add(null);
			}

		}

		@Override
		public void flatMap(Edge<Long, NullValue> edge, Collector<SampledEdge> out) throws Exception {
			this.edgeCount++;

			// Flip a coin for all instances
			for (int i = 0; i < instanceSize * p; ++i) {
				boolean sample = Coin.flip(this.edgeCount, randoms.get(i));
				int subtask = i % p;
				int instance = i / p;

				if (sample) {
					out.collect(new SampledEdge(subtask, instance, edge, edgeCount, true));
					samples.set(i, edge);

					// emitCount++;
				} else if (samples.get(i) != null) {
					// Check if the edge is incident to the sampled one
					Edge<Long, NullValue> e = samples.get(i);
					boolean incidence = e.getSource().equals(edge.getSource())
							|| e.getSource().equals(edge.getTarget())
							|| e.getTarget().equals(edge.getSource())
							|| e.getTarget().equals(edge.getTarget());

					if (incidence) {
						out.collect(new SampledEdge(subtask, instance, edge, edgeCount, false));
						// emitCount++;
					}
				}
			}

			/*
			if (edgeCount % 1000 == 0) {
				System.out.printf("Emit rate: %.2f\n", (double) emitCount / (double) edgeCount);
			}
			*/
		}
	}

	@SuppressWarnings("serial")
	private static final class TriangleSampleMapper extends RichFlatMapFunction<SampledEdge, TriangleEstimate> {
		private List<SampleTriangleState> states;
		private int edgeCount;
		private int previousResult;
		private int vertices;

		public TriangleSampleMapper(int size, int vertices) {
			this.states = new ArrayList<>();
			this.edgeCount = 0;
			this.previousResult = 0;
			this.vertices = vertices;

			for (int i = 0; i < size; ++i) {
				states.add(new SampleTriangleState());
			}
		}

		@Override
		public void flatMap(SampledEdge input, Collector<TriangleEstimate> out) throws Exception {
			Edge<Long, NullValue> edge = input.getEdge();

			// Update edge count
			edgeCount = input.getEdgeCount();

			SampleTriangleState state = states.get(input.getInstance());

			// With probability 1/i sample a candidate (already flipped the coin during partitioning)
			if (input.isResampled()) {
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

			// Update beta
			boolean triangleFound = false;
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

				triangleFound = (state.srcEdgeFound && state.trgEdgeFound);
				state.beta = triangleFound ? 1 : 0;
			}

			// Sum local betas
			if (triangleFound) {
				int localBetaSum = 0;
				for (SampleTriangleState s : states) {
					localBetaSum += s.beta;
				}

				if (localBetaSum != previousResult) {
					previousResult = localBetaSum;

					int source = getRuntimeContext().getIndexOfThisSubtask();
					out.collect(new TriangleEstimate(source, edgeCount, localBetaSum));
				}
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
		public static boolean flip(int size, Random rnd) {
			return rnd.nextDouble() * size <= 1.0;
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
				System.err.println("Usage: IncidenceSamplingTriangleCount <input edges path> <output path> <vertex count> <sample count>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];
			vertexCount = Integer.parseInt(args[2]);
			samples = Integer.parseInt(args[3]);
		} else {
			System.out.println("Executing IncidenceSamplingTriangleCount example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("  Usage: IncidenceSamplingTriangleCount <input edges path> <output path> <vertex count> <sample count>");
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
		return "Incidence Sampling Triangle Count";
	}
}