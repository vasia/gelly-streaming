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

		for (int v = 1000; v <= 10000; v += 1000) {
			Tuple2<Integer, Integer> config = new Tuple2<>(v, v * 25);

			for (int t = 0; t < 1; ++t) {
				System.out.printf("Processing %d, try %d, Incidence\n", v, t);

				String graphFile = String.format("graphs/graph_%d.txt", v);

				// Run Incidence sampling version
				IncidenceSamplingTriangleCount incidence = new IncidenceSamplingTriangleCount();
				result = incidence.run(graphFile, v);
				append("incidence", config, result);
				System.out.printf("\tResult: %d, %d\n", result.f0, result.f1);

				// -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

				System.out.printf("Processing %d, try %d, Broadcast\n", v, t);

				// Run broadcast version
				BroadcastTriangleCount broadcast = new BroadcastTriangleCount();
				result = broadcast.run(graphFile, v);
				append("broadcast", config, result);

				System.out.printf("\tResult: %d, %d\n", result.f0, result.f1);
			}
		}
	}
}
