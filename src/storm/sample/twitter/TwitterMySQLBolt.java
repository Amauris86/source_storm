package storm.sample.twitter;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * @author Este Bolt permite crear un nuevo registro en la tabla
 *         tweet_score en la base de datos db_twitter_storm, con el tweet
 *         procesado.
 */
public class TwitterMySQLBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	MySQLDB dbcon;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		dbcon = new MySQLDB();
	}

	@Override
	public void execute(Tuple input) {
		Long idTweet = (Long) input.getValueByField("tweet_id");
		String textTweet = String.valueOf(input.getValueByField("tweet_text"));
		Float pos = input.getFloat(input.fieldIndex("pos_score"));
		Float neg = input.getFloat(input.fieldIndex("neg_score"));
		Integer sentTweet = input.getInteger(input.fieldIndex("sentiment_tweet"));
		dbcon.create_item(idTweet, textTweet, pos, neg, sentTweet);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
