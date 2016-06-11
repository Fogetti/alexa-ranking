package alexa.ranking.integration;

import org.apache.storm.generated.StormTopology;

public class AlexaTopologyEnd2End {

	public static void main(String[] args) throws Exception {
		String urlDataFile = "/Users/fogetti/Work/backup/phish-result-2016-06-10/phishing-result.csv";
        String proxyDataFile = "/Users/fogetti/Work/fogetti-phish-ansible/input/working-proxies.txt";
		String resultDataFile = "/Users/fogetti/Work/alexa-result.csv";
		String ackedDataFile = "/Users/fogetti/Work/alexa-acked.csv";
		StormTopology topology = AlexaTopologyBuilder.build(urlDataFile, ackedDataFile, proxyDataFile, resultDataFile);
		AlexaLocalRunner.run(args, topology);
	}

}
