package storm.sample.twitter;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

/**
 * @author Permite definir la topologia a desplegar en Apache Storm en
 *         cluster. En esta topologia se define un Spout, el cual se encargara
 *         de obtener los tweets que seran procesados. Tambien se definen 4
 *         Bolts, 2 de ellos se encargaran de procesar el tweet y los otros se
 *         encargaran de almacenar el resultado del tweet procesado en una base
 *         de datos y en el sistema de archivos del sistema operativo.
 */
public class TwitterTopology {
	
	final static String TOPOLOGY_NAME = "twitter_storm_sentimental_data";

	public static void main(String[] args) {
		Config config = new Config();
		config.setMessageTimeoutSecs(120);
		config.setDebug(true);
		TopologyBuilder tb = new TopologyBuilder();
		tb.setSpout("TwitterSpout", new TwitterSpout());
		tb.setBolt("TwitterFilterBolt", new TwitterFilterBolt())
				.shuffleGrouping("TwitterSpout");
		tb.setBolt("TwitterValidateText", new TwitterValidateTextBolt())
				.shuffleGrouping("TwitterFilterBolt");
		tb.setBolt("TwitterLocalFilesBolt", new TwitterLocalFilesBolt())
				.shuffleGrouping("TwitterValidateText");
		tb.setBolt("TwitterMySQLBolt", new TwitterMySQLBolt()).shuffleGrouping(
				"TwitterValidateText");
		try {
			StormSubmitter.submitTopology(TOPOLOGY_NAME, config,
					tb.createTopology());
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
		}
	}

}
