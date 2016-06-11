package alexa.ranking.topology;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeSet;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListReadingSpout extends BaseRichSpout {

    private static final Logger logger = LoggerFactory.getLogger(ListReadingSpout.class);
    private static final long serialVersionUID = 4461080581264998777L;
    private final String urlDataFile;
    private SpoutOutputCollector collector;
    private TreeSet<String> urls;

    public ListReadingSpout(String urlDataFile) {
        this.urlDataFile = urlDataFile;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        try {
            urls = new TreeSet<>(Files.readAllLines(Paths.get(urlDataFile)));
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void nextTuple() {
        if (!urls.isEmpty()) {
            String line = urls.last();
            urls.remove(line);
            logger.info("Emitting [{}]", line);
            collector.emit(new Values(line), line);
        }
    }

    @Override
    public void ack(Object msgId) {
        logger.info("Acking [{}]", msgId);
        try {
            Files.write(Paths.get(urlDataFile), urls);
        } catch (IOException e) {
            logger.error("Could not rewrite the URL file", e);
        }
    }

    @Override
    public void fail(Object msgId) {
        logger.debug("Message [msg={}] failed", msgId);
        urls.add(msgId.toString());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

}
