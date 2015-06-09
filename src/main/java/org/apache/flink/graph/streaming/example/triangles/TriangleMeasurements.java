package org.apache.flink.graph.streaming.example.triangles;


import org.apache.flink.api.java.tuple.Tuple2;

import java.io.FileWriter;

public class TriangleMeasurements {

	public static void append(String file, Tuple2<Integer, Integer> config,
			Tuple2<Long, Long> result) throws Exception {
		FileWriter fp = new FileWriter(file + "_results.csv", true);
		fp.append(String.format("%d, %d, %d, %d\n", config.f0, config.f1, result.f0, result.f1));
		fp.close();
	}

	public static void main(String[] args) throws Exception {
		Tuple2<Long, Long> result;

		if (args.length < 5) {
			System.out.println("Arguments: <srcDir> <srcFmt> <start size> <end size> <increment>");
			return;
		}

		String graphDir = args[0];
		String graphFormat = args[1];
		int startSize = Integer.parseInt(args[2]);
		int endSize = Integer.parseInt(args[3]);
		int increment = Integer.parseInt(args[4]);

		for (int v = startSize; v <= endSize; v += increment) {
			System.out.print("Processing size " + v);

			Tuple2<Integer, Integer> config = new Tuple2<>(v, v * 25);

			String graphFile = String.format("%s/%s", graphDir, String.format(graphFormat, v));

			// Run Incidence sampling version
			IncidenceSamplingTriangleCount incidence = new IncidenceSamplingTriangleCount();
			result = incidence.run(graphFile, v);
			append("incidence", config, result);
			System.out.printf("\tResult: %d, %d\n", result.f0, result.f1);

			// -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

			// Run broadcast version
			BroadcastTriangleCount broadcast = new BroadcastTriangleCount();
			result = broadcast.run(graphFile, v);
			append("broadcast", config, result);
		}
	}
}
