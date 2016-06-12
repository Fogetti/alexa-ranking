package alexa.ranking.integration;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;

public class AlexaRemoteRunner {

    public static void main(String[] args) throws Exception {
        String urlDataFile = args[0];
        String proxyDataFile = args[1];
        String resultDataFile = args[2];
        String ackedDataFile = args[3];
        StormTopology topology = AlexaTopologyBuilder.build(urlDataFile, ackedDataFile, proxyDataFile, resultDataFile);
        
        Config config = new Config();
        config.setNumWorkers(60);
        config.setMessageTimeoutSecs(180);
        config.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE,
                   new Integer(131072));
        config.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE,
                   new Integer(131072));
        config.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE,
                   new Integer(131072));
        
        StormSubmitter.submitTopology("alexa-ranking", config, topology);
    }
    
}
