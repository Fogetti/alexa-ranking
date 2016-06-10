package alexa.ranking.topology;

import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.storm.topology.IRichBolt;

import com.gargoylesoftware.htmlunit.BrowserVersion;
import com.gargoylesoftware.htmlunit.HttpWebConnection;
import com.gargoylesoftware.htmlunit.NicelyResynchronizingAjaxController;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.WebConnection;

public class ClientHoldingRankingRequestBolt extends RankingRequestBolt implements IRichBolt {

    private static final long serialVersionUID = -6380393643116704611L;

    public ClientHoldingRankingRequestBolt(String resultDataFile, String proxyDataFile) {
        super(resultDataFile, proxyDataFile);
    }

    @Override
    public WebClient buildClient() {
        WebClient webClient = new WebClient(BrowserVersion.FIREFOX_38);
        WebConnection webCon = new NoRetryWebConnection(webClient);
        webClient.setWebConnection(webCon);
        webClient.setAjaxController(new NicelyResynchronizingAjaxController());
        webClient.getOptions().setTimeout((int)timeout);
        webClient.getOptions().setThrowExceptionOnScriptError(false);
        webClient.getOptions().setThrowExceptionOnFailingStatusCode(false);
        webClient.getOptions().setCssEnabled(true);
        webClient.getOptions().setRedirectEnabled(true);
        return webClient;
    }
    
    private static class NoRetryWebConnection extends HttpWebConnection {

        public NoRetryWebConnection(WebClient webClient) {
            super(webClient);
        }
        
        @Override
        protected HttpClientBuilder getHttpClientBuilder() {
            HttpClientBuilder builder = super.getHttpClientBuilder();
            builder.setRetryHandler(new DefaultHttpRequestRetryHandler(0, false));
            return builder;
        }
        
        @Override
        protected HttpClientBuilder createHttpClient() {
            HttpClientBuilder builder = super.createHttpClient();
            builder.setRetryHandler(new DefaultHttpRequestRetryHandler(0, false));
            return builder;
        }
        
    }

}
