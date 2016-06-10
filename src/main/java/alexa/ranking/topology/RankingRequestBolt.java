package alexa.ranking.topology;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;

import org.apache.commons.lang3.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gargoylesoftware.htmlunit.FailingHttpStatusCodeException;
import com.gargoylesoftware.htmlunit.ProxyConfig;
import com.gargoylesoftware.htmlunit.WebClient;

import alexa.ranking.client.AlexaRanking;

public abstract class RankingRequestBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(RankingRequestBolt.class);
    private static final long serialVersionUID = 3590180782929238293L;
    private final String resultDataFile;
    private final String proxyDataFile;
    private OutputCollector collector;
    private List<String> proxies;
    private Path result;
    
    protected long timeout;

    static {
        java.util.logging.Logger.getLogger("com.gargoylesoftware").setLevel(Level.INFO); 
    }

    public RankingRequestBolt(String resultDataFile, String proxyDataFile) {
        this.resultDataFile = resultDataFile;
        this.proxyDataFile = proxyDataFile;
    }

    public abstract WebClient buildClient();
    
    @Override
    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        timeout = (Long)stormConf.get("timeout");
        result = Paths.get(resultDataFile);
        try {
            proxies = Files.readAllLines(Paths.get(proxyDataFile));
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void execute(Tuple input) {
        String line = input.getStringByField("line");
        String link = StringUtils.substringAfter(line, "\"http:");
        if (!StringUtils.isBlank(link)) link = "http:" + StringUtils.removeEnd(link, "\"");
        else link = "https:" + StringUtils.removeEnd(StringUtils.substringAfter(line, "\"https:"), "\"");
        logger.info("Ranking: [{}]", link);
        try {
            calculateSearches(link, line);
            collector.ack(input);
        } catch (IOException e) {
            logger.error("Calculating ranking for: "+link+" failed", e);
            collector.fail(input);
        } catch (NullPointerException e) {
            logger.error("Calculating ranking for: "+link+" failed", e);
            collector.fail(input);
        } catch (FailingHttpStatusCodeException e) {
            logger.error("Calculating ranking for: "+link+" failed", e);
            collector.fail(input);
        }
    }

    private void calculateSearches(String url, String line) throws IOException {
        int nextPick = new Random().nextInt(proxies.size());
        String nextProxy = proxies.get(nextPick);
        String[] hostAndPort = nextProxy.split(":");
        ProxyConfig proxyConfig = new ProxyConfig(hostAndPort[0],Integer.parseInt(hostAndPort[1]));
        AlexaRanking.Builder builder = new AlexaRanking.Builder(proxyConfig)
                .setHttpClient(buildClient())
                .setConnectTimeout((int)timeout);
        AlexaRanking client = builder.build();
        String ranking = client.rankingFor(URLEncoder.encode(url, "UTF-8"));
        if (ranking == null) throw new NullPointerException("Ranking was null");
        String[] rnk = {line+","+ranking};
        Files.write(result, Arrays.asList(rnk), UTF_8, WRITE, APPEND, CREATE);
        logger.debug("Alexa ranking request succeeded");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}