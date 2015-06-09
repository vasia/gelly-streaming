package org.apache.flink.graph.streaming.example.bipartiteness;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;

public class BipartiteMeasurement {

	private static String srcFile;
	private static String tmpFile = "tmp_graph.txt";

	private static void createData(int size) throws Exception {
		// Create new data set;
		FileWriter fw = new FileWriter(tmpFile, false);

		try (BufferedReader br = new BufferedReader(new FileReader(srcFile))) {
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
	}

	private static void measureWindowed(int size, int window) throws Exception{
		String windowResult = "bipartition_window_results.csv";

		WindowedBipartiteMergeTree w = new WindowedBipartiteMergeTree();
		long timeResult = w.run(tmpFile, window);

		FileWriter res = new FileWriter(windowResult, true);
		res.append(String.format("%d, %d, %d\r\n", size, window, timeResult));
		res.close();
	}

	private static void measureStreamed(int size) throws Exception{
		String serialResult = "bipartition_serial_results.csv";

		StreamedBipartiteness s = new StreamedBipartiteness();
		long timeResult = s.run(tmpFile);

		FileWriter res = new FileWriter(serialResult, true);
		res.append(String.format("%d, %d\r\n", size, timeResult));
		res.close();
	}

	public static void main(String[] args) throws Exception {

		if (args.length < 4) {
			System.out.println("Arguments: <srcFile> <start size> <end size> <increment>");
			return;
		}

		srcFile = args[0];
		int startSize = Integer.parseInt(args[1]);
		int endSize = Integer.parseInt(args[2]);
		int increment = Integer.parseInt(args[3]);

		// Measure different sizes
		for (int size = startSize; size <= endSize; size += increment) {
			System.out.printf("Running size %d\n", size);

			createData(size);

			// Measure for two different windows
			for (int window = 10000; window <= 1000000; window *= 10) {
				measureWindowed(size, window);
			}

			// Measure streamed version
			measureStreamed(size);
		}
	}
}
