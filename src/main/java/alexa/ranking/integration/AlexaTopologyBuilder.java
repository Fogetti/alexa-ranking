package alexa.ranking.integration;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import alexa.ranking.topology.ClientHoldingRankingRequestBolt;
import alexa.ranking.topology.ListReadingSpout;


public class AlexaTopologyBuilder {

    public static StormTopology build() throws Exception {
        String urlDataFile = System.getProperty("alexa.url.file");
        String ackedDataFile = System.getProperty("alexa.acked.file");
        String proxyDataFile = System.getProperty("alexa.proxy.file");
        String resultDataFile = System.getProperty("alexa.result.file");
        return build(urlDataFile, ackedDataFile, proxyDataFile, resultDataFile);
    }

    public static StormTopology build(
            String urlDataFile,
            String ackedDataFile,
            String proxyDataFile,
            String resultDataFile) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder
            .setSpout("listsource", new ListReadingSpout(urlDataFile, ackedDataFile), 1)
            .setMaxSpoutPending(5000)
            .setNumTasks(1);
        builder.setBolt("ranking", new ClientHoldingRankingRequestBolt(resultDataFile, proxyDataFile), 1024)
            .addConfiguration("timeout", 45000)
            .fieldsGrouping("listsource", new Fields("line"))
            .setNumTasks(1024);
        StormTopology topology = builder.createTopology();
        return topology;
    }

}