package storm.sample.twitter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Tuple;

/**
 * @author Este Bolt realiza el registro del tweet procesado guardandolo
 *         en el sistema de archivos del sistema operativo. Un archivo es creado
 *         el sistema de archivos cuando se han completado 10 tweets procesados.
 *         La estructura de directorios se gestiona de la siguiente manera
 *         TWEET_DIRECTORY/YEAR/MONTH/DAY/HOUR cambiando de directorio cada
 *         hora. El TWEET_DIRECTORY se encuentra en el directorio
 *         home_user/tweets.
 */
public class TwitterLocalFilesBolt extends BaseRichBolt {
	private OutputCollector collector;
	private List<String> regsTweets;
	private static final String SEPARATOR = System
			.getProperty("file.separator");

	// define el directorio en el cual se van a guardar los tweets.
	private static final String TWEET_DIRECTORY = System
			.getProperty("user.home") + SEPARATOR + "tweets";

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.regsTweets = new ArrayList<String>(5);
	}

	public void execute(Tuple input) {
		Long idTweet = (Long) input.getValueByField("tweet_id");
		String textTweet = String.valueOf(input.getValueByField("tweet_text"));
		Float pos = input.getFloat(input.fieldIndex("pos_score"));
		Float neg = input.getFloat(input.fieldIndex("neg_score"));
		Integer sentTweet = input.getInteger(input.fieldIndex("sentiment_tweet"));
		textTweet = textTweet.replace("\n", " ");
		String regTweet = String.format("%s,%s,%f,%f,%s\n", idTweet, textTweet,
				pos, neg, sentTweet);
		this.regsTweets.add(regTweet);

		// se realiza la escritura cada 10 tweets
		if (this.regsTweets.size() == 10) {
			writeToFile();
			this.regsTweets.clear();
		}
	}

	/**
	 * Permite realizar la escritura del archivo en el sistema de archivos del
	 * SO, los archivos son guardados en directorios cada hora, mantiene la
	 * siguiente estructura TWEET_DIRECTORY/YEAR/MONTH/DAY/HOUR
	 */
	private void writeToFile() {
		Calendar now = Calendar.getInstance();
		String year = String.valueOf(now.get(Calendar.YEAR));
		String month = String.valueOf(now.get(Calendar.MONTH) + 1);
		String day = String.valueOf(now.get(Calendar.DAY_OF_MONTH));
		String hour = String.valueOf(now.get(Calendar.HOUR_OF_DAY));
		File directory = new File(TWEET_DIRECTORY + SEPARATOR + year
				+ SEPARATOR + month + SEPARATOR + day + SEPARATOR + hour);
		if (!directory.exists())
			directory.mkdirs();
		DateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
		try {
			PrintWriter tweet_file = new PrintWriter(directory.getPath()
					+ SEPARATOR + "storm_tweets_" + df.format(now.getTime()),
					"UTF-8");
			for (String tweet : regsTweets)
				tweet_file.write(tweet);
			tweet_file.close();
		} catch (FileNotFoundException | UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}
