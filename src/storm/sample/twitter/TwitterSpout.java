package storm.sample.twitter;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * @author Este Spout permite obtener el streaming de twitter a traves
 *         del api Twitter4J, definiendo las palabras clave que se quiere hacer
 *         seguimiento.
 */
public class TwitterSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	private TwitterStream twitterStream;
	private SpoutOutputCollector collector;
	private LinkedBlockingQueue<Status> queue;

	@Override
	public void open(Map conf, TopologyContext contex,
			SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<Status>(1000);
		this.collector = collector;

		ConfigurationBuilder cb = new ConfigurationBuilder();

		// Definir los Keys and Access Tokens de la aplicacion creada en
		// Twitter.
		cb.setOAuthConsumerKey("YX4tQ6z14ge5vVrgBU4uG8RbC")
				.setOAuthConsumerSecret(
						"rBO8TqIIp29BD9OUE4TMgWvtibbVPCTFHfpIHg9NpdbkJiPEtg")
				.setOAuthAccessToken(
						"776344706062114816-a2pD8SWAvptDEDpu04cv1lmbADgIAXT")
				.setOAuthAccessTokenSecret(
						"4evcvUpTlRNMBxqLTN8mbAuD2MCuRBwiqZGv20LoiNaX5");

		TwitterStream twitterStream = new TwitterStreamFactory(cb.build())
				.getInstance();
		twitterStream.addListener(new StatusListener() {
			public void onStatus(Status status) {
				queue.offer(status);
			}

			@Override
			public void onException(Exception e) {
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice sdn) {
			}

			@Override
			public void onScrubGeo(long l1, long l2) {
			}

			@Override
			public void onStallWarning(StallWarning sw) {
			}

			@Override
			public void onTrackLimitationNotice(int i) {
			}
		});

		FilterQuery tweetFilterQuery = new FilterQuery();

		// Definir las palabras clave hacer seguimiento.
		tweetFilterQuery.track(new String[] { "MovistarPlus", "JuegoDeTronos",
				"BreakingBad", "BetterCallSaul", "HouseOfCards",
				"TrueDetective" });
		twitterStream.filter(tweetFilterQuery);
	}

	@Override
	public void close() {
		twitterStream.shutdown();
	}

	@Override
	public void nextTuple() {
		Status tweet = queue.poll();
		if (tweet == null)
			Utils.sleep(50);
		else
			collector.emit(new Values(tweet));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}
}