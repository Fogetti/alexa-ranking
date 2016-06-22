package alexa.ranking.misc;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;

public class LoadAlexaRanking {

    private static Encoder encoder = Base64.getEncoder();

    public static void main(String[] args) throws IOException, URISyntaxException {
        Path path = Paths.get(
                LoadAlexaRanking
                .class
                .getClassLoader()
                .getResource("phishing-result.csv")
                .toURI());
        Path result = Paths.get(
                LoadAlexaRanking
                .class
                .getClassLoader()
                .getResource(".")
                .toURI())
                .resolveSibling("redis-commands.txt");
        CSVParser parser = CSVFormat.DEFAULT.withSkipHeaderRecord().parse(
                Files.newBufferedReader(path, StandardCharsets.UTF_8));
        List<String> commands = new ArrayList<>();
        for (CSVRecord line : parser) {
            String ranking = line.get(11);
            String URL = line.get(12);
            if (ranking != null && !"null".equals(ranking)) {
                String registeredURL = findRegisteredURL(URL);
                System.out.println(String.format("Adding ranking %s for URL %s to the redis command list", ranking, registeredURL));
                commands.add("SET \"alexa:"+encode(registeredURL)+"\" \""+ranking+"\"");
            }
        }
        System.out.println(String.format("Writing results to [%s]", result.toString()));
        Files.write(result, commands, StandardCharsets.UTF_8, StandardOpenOption.WRITE, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
    }

    private static String findRegisteredURL(String URL) {
        String[] urlParts = URL.split("//");
        String protocol = urlParts[0];
        String URLPostFix = urlParts[1];
        String URLPrefix = StringUtils.substringBefore(URLPostFix, "/");
        return protocol+"//"+URLPrefix;
    }

    private static String encode(String URL) {
        String encodedURL = encoder.encodeToString(URL.getBytes(StandardCharsets.UTF_8));
        return encodedURL;
    }

}