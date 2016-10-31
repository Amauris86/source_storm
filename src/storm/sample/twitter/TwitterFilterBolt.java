package storm.sample.twitter;

import java.util.Map;
import twitter4j.Status;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * @author Este Bolt reliza un filtro al texto del tweet, removiendo los caracteres
 *         que no son alfabeticos.
 */
public class TwitterFilterBolt extends BaseRichBolt {

	private static final long serialVersionUID = -5340603788012805394L;
	private OutputCollector collector;

	@Override
	public void execute(Tuple input) {
		Status tweet = (Status) input.getValueByField("tweet");
		Long idTweet = tweet.getUser().getId();
		
		//reemplazar los caracteres que no son alfabeticos. 
		String textTweet = tweet.getText()
				.replaceAll("[^a-zA-ZÃ±Ã‘Ã¡Ã�Ã©Ã‰Ã­Ã�Ã³Ã“ÃºÃš\\s]", "").trim()
				.toLowerCase();
		collector.emit(new Values(idTweet, textTweet, tweet.getText()));
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet_id", "tweet_text", "tweet_text_orig"));
	}
}