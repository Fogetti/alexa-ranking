package alexa.ranking.client;

import java.io.IOException;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Parser;

import com.gargoylesoftware.htmlunit.NicelyResynchronizingAjaxController;
import com.gargoylesoftware.htmlunit.Page;
import com.gargoylesoftware.htmlunit.ProxyConfig;
import com.gargoylesoftware.htmlunit.TextPage;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.gargoylesoftware.htmlunit.xml.XmlPage;

public class AlexaRanking {

    private final ProxyConfig proxyConfig;
    private final WebClient client;
    private final int timeout;

    public static class Builder {

        private final ProxyConfig proxyConfig;
        private WebClient client;
        private int timeout;

        public Builder(ProxyConfig proxyConfig) {
            this.proxyConfig = proxyConfig;
        }

        public Builder setHttpClient(WebClient client) {
            this.client = client;
            return this;
        }

        public Builder setConnectTimeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public AlexaRanking build() {
            return new AlexaRanking(proxyConfig, client, timeout);
        }

    }

    private AlexaRanking(ProxyConfig proxyConfig, WebClient client, int timeout) {
        this.proxyConfig = proxyConfig;
        this.client = client;
        this.timeout = timeout;
    }

    public String rankingFor(String url) throws IOException {
        String query
            = String.format("http://data.alexa.com/data?cli=10&url=%s", url);
        String xml = buildXml(query);
        Document doc = Jsoup.parse(xml, "", Parser.xmlParser());
        for (Element e : doc.select("POPULARITY")) {
            return e.attr("TEXT");
        }
        return null;
    }

    private String buildXml(String query) throws IOException {
        client.setAjaxController(new NicelyResynchronizingAjaxController());
        client.getOptions().setTimeout(timeout);
        client.getOptions().setProxyConfig(proxyConfig);
        client.getOptions().setThrowExceptionOnScriptError(false);
        client.getOptions().setThrowExceptionOnFailingStatusCode(false);
        client.getOptions().setCssEnabled(true);
        client.getOptions().setRedirectEnabled(true);
        Page page = client.getPage(query);

        client.waitForBackgroundJavaScript(10000);
        client.waitForBackgroundJavaScriptStartingBefore(10000);

        if (page.isHtmlPage()) {
            final HtmlPage htmlPage = (HtmlPage) page;
            final String html = htmlPage.asXml();
            return html;
        } else if (page instanceof XmlPage) {
            final XmlPage xmlPage = (XmlPage) page;
            final String xml = xmlPage.asXml();
            return xml;
        } else if (page instanceof TextPage) {
            final TextPage textPage = (TextPage) page;
            final String text = textPage.getContent();
            return text;
        } else {
            return "";
        }
    }

}