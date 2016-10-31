package storm.sample.twitter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * @author Este Bolt permite clasificar las palabras usadas en un tweet
 *         en positivas o negativas. El Tweet se clasifica como positivo(1),
 *         negativo(-1) o neutro(0) de acuerdo al promedio de palabras positivas
 *         y negativas que tiene el tweet.
 *
 */
public class TwitterValidateTextBolt extends BaseRichBolt {

	private static final long serialVersionUID = 9004481475544944930L;
	private OutputCollector collector;

	@Override
	public void execute(Tuple input) {
		Long idTweet = (Long) input.getValueByField("tweet_id");
		String[] wordsTweet = String.valueOf(
				input.getValueByField("tweet_text")).split(" ");
		String textTweetOri = String.valueOf(input
				.getValueByField("tweet_text_orig"));

		// obtener los diccionarios de palabras
		Set<String> negativeWords = DictionaryWords.getNegativeWords();
		Set<String> positiveWords = DictionaryWords.getPositiveWords();

		int numWords = wordsTweet.length;
		int numPosWords = 0;
		int numNegWords = 0;

		// contabilizar la cantidad de palabras positivas y negativas en el
		// texto del tweet.
		List<String> n = new ArrayList<String>();
		List<String> p = new ArrayList<String>();
		
		for (String word : wordsTweet) {
			word = word.trim();
			if (negativeWords.contains(word)){
				numNegWords++;
				n.add(word);
			}if (positiveWords.contains(word)){
				numPosWords++;
				p.add(word);
			}
		}
		
		// promedio de palabras positivas y negativas en el tweet.
		Float avgPos = (float) numPosWords / numWords;
		Float avgNeg = (float) numNegWords / numWords;

		// clasifica el tweet como positivo(1), negativo(-1) o neutro(0)
		int sentiment_tweet = 0;
		if (avgPos > avgNeg)
			sentiment_tweet = 1;
		else {
			if (avgPos < avgNeg)
				sentiment_tweet = -1;
		}

		collector.emit(new Values(idTweet, textTweetOri, avgPos, avgNeg,
				sentiment_tweet));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet_id", "tweet_text", "pos_score",
				"neg_score", "sentiment_tweet"));
	}

	@Override
	public void prepare(Map map, TopologyContext topologyContext,
			OutputCollector collector) {
		this.collector = collector;
	}
}