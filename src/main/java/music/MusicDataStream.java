package music;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Vertex;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.WindowMapFunction;
import org.apache.flink.streaming.api.windowing.helper.Count;
import org.apache.flink.util.Collector;
import triangles.Triplet;

import java.text.SimpleDateFormat;
import java.util.Iterator;

public class MusicDataStream {

	public MusicDataStream() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
		long id = 1000L;

		// Get it from: http://www.dtic.upf.edu/~ocelma/MusicRecommendationDataset/lastfm-1K.html
		DataStream<Triplet<Vertex<Long, String>, Long>> stream = env.readTextFile("lastfm_small_sorted.txt")
				.flatMap(new FlatMapFunction<String, Triplet<Vertex<Long, String>, Long>>() {
					@Override
					public void flatMap(String s, Collector<Triplet<Vertex<Long, String>, Long>> out) throws Exception {

						// Parse lines from the text file
						String[] args = s.split("\t");
						long userId = Long.parseLong(args[0].substring(5));
						long timestamp = dateFormat.parse(args[1]).getTime();
						String artistWithSong = args[3].concat(" - ").concat(args[5]);

						Vertex<Long, String> user = new Vertex<>(userId, "user_" + Long.toString(userId));
						Vertex<Long, String> song = new Vertex<>(Integer.MAX_VALUE + (long) artistWithSong.hashCode(),
								artistWithSong);

						out.collect(new Triplet<>(user, timestamp, song));
					}
				});

		stream.window(Count.of(1000000)).groupBy(2).mapWindow(
				new WindowMapFunction<Triplet<Vertex<Long,String>,Long>, Tuple2<String, Long>>() {
			@Override
			public void mapWindow(Iterable<Triplet<Vertex<Long, String>, Long>> iterable,
					Collector<Tuple2<String, Long>> out) throws Exception {

				Iterator<Triplet<Vertex<Long, String>, Long>> iterator = iterable.iterator();

				long sum = 1L;
				String key = iterator.next().getTrgVertexValue().getValue();

				while (iterator.hasNext()) {
					iterator.next();
					sum++;
				}

				out.collect(new Tuple2<>(key, sum));
			}
		}).max(1).flatten().print();

		env.execute("Music Data Stream");
	}

	public static void main(String[] args) throws Exception {
		new MusicDataStream();
	}
}
