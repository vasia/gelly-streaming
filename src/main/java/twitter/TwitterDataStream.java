package twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.sling.commons.json.JSONArray;
import org.apache.sling.commons.json.JSONObject;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterDataStream {

	public TwitterDataStream() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

		Mention typeSample = new Mention(new Vertex<>(0L, new User("user0")), new Edge<>(0L, 1L, 2L),
				new Vertex<>(1L, new User("user1")));

		TypeInformation<Vertex<Long, User>> vertexType = TypeExtractor.getForObject(typeSample.getMentioner());
		TypeInformation<Edge<Long, Long>> edgeType = TypeExtractor.getForObject(typeSample.getEdge());
		TypeInformation<Mention> mentionType = new TupleTypeInfo<>(vertexType, edgeType, vertexType);

		DataStream<Mention> stream = env.addSource(new TweetSourceFunction())
				.setType(mentionType);


		/*
		 * Most mentioned user per window
		 */
		/*
		stream.window(Count.of(100)).groupBy(1).mapWindow(new WindowMapFunction<Tuple3<Long,Long,Long>,
				Tuple2<Long, Long>>() {
			@Override
			public void mapWindow(Iterable<Tuple3<Long, Long, Long>> iterable,
					Collector<Tuple2<Long, Long>> out) throws Exception {
				long key = 0L;
				long count = 0L;
				for (Tuple3<Long, Long, Long> mention : iterable) {
					key = mention.f1;
					count++;
				}
				out.collect(new Tuple2<>(key, count));
			}
		}).maxBy(1).flatten().print();
		*/

		stream.print();
		
		env.execute("Twitter Data Stream");
	}

	public static final class TweetSourceFunction
			implements SourceFunction<Mention> {

		@Override
		public void run(Collector<Mention> out) throws Exception {

			// Set up hosebird
			BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);
			BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>(1000);

			// Connection and auth
			Hosts host = new HttpHosts(Constants.STREAM_HOST);
			StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();

			List<String> terms = Lists.newArrayList("RT");
			endpoint.trackTerms(terms);

			Authentication auth = new OAuth1("CRDprJ1oRAb2WmKgWeQgUisz4",
					"JCXmokxhAXe0aoLpB8SzMQQ35UHPzDOcNYC9E500VIUfJ2u6Mz",
					"81405713-ZLU0hflIQomtGxGxYdQ6bD7ikYPDZg4koVbY3uQ3r",
					"TYHScK8LEADKdi4Ayu4lgLqQzdWG4eZKjxwif0oV8kas9");

			// Building a client
			Client client = new ClientBuilder()
					.name("hosebird-client")
					.hosts(host)
					.authentication(auth)
					.endpoint(endpoint)
					.processor(new StringDelimitedProcessor(msgQueue))
					.eventMessageQueue(eventQueue)
					.build();

			client.connect();

			while (true) {
				String msg = msgQueue.take();

				JSONObject tweet = new JSONObject(msg);

				// Only new tweets are interesting
				if (!tweet.has("created_at")) {
					continue;
				}

				long timestamp = tweet.getLong("timestamp_ms");

				JSONObject user = tweet.getJSONObject("user");
				Vertex<Long, User> fromUser = new Vertex<>(user.getLong("id"),
						new User(user.getString("screen_name")));

				JSONObject entities = tweet.getJSONObject("entities");
				JSONArray mentions = entities.getJSONArray("user_mentions");

				for (int i = 0; i < mentions.length(); ++i) {
					JSONObject mention = mentions.getJSONObject(i);
					Vertex<Long, User> toUser = new Vertex<>(mention.getLong("id"),
							new User(mention.getString("screen_name")));

					Edge<Long, Long> edge = new Edge<>(fromUser.getId(), toUser.getId(), timestamp);

					out.collect(new Mention(fromUser, edge, toUser));
				}
			}
		}

		@Override
		public void cancel() {

		}
	}

	public static final class Mention
		extends Tuple3<Vertex<Long, User>, Edge<Long, Long>, Vertex<Long, User>> {

		public Mention(Vertex<Long, User> from, Edge<Long, Long> edge, Vertex<Long, User> to) {
			this.f0 = from;
			this.f1 = edge;
			this.f2 = to;
		}

		public Vertex<Long, User> getMentioner() {
			return f0;
		}

		public Edge<Long, Long> getEdge() {
			return f1;
		}

		public Vertex<Long, User> getMentionee() {
			return f2;
		}
	}

	public static final class User implements Serializable {

		private String name;

		public User(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}

		@Override
		public String toString() {
			return String.format("[%s]", name);
		}
	}

	public static void main(String[] args) throws Exception {
		new TwitterDataStream();
	}
}
