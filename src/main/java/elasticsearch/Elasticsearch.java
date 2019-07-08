package elasticsearch;

import org.apache.http.HttpHost;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;

import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Elasticsearch {

    private static final String HOST_ENV_VAR = "ELASTICSEARCH_HOST";
    private static final String INDEX_ENV_VAR = "ELASTICSEARCH_INDEX";
    private static final int MAX_UPDATES = 5000;

    private static AtomToRequest converter = new AtomToRequest();
    RestHighLevelClient client;

    //ran with airflow
    public static void main(String[] args) {
        try {
            Elasticsearch es = new Elasticsearch();
            es.update();
            es.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //constructor to connect to cluster
    public Elasticsearch() throws IOException {

        try {
            client = new RestHighLevelClient(
                RestClient.builder( new HttpHost(HOST_ENV_VAR, 9200, "http") )
            );

            System.out.println("Connected to Elasticsearch");
            System.out.println("Version " + client.info(RequestOptions.DEFAULT).getVersion());
        } catch (IOException exception) {
            System.out.println("Couldn't connect to Elasticsearch");
            exception.printStackTrace();
        }
    }

    //create new request from YouTube push notification
    //return the video id
    public String newRequest(String body) throws Exception {
        if (body == null) return null;
        String vidId = null;
        try{
            //get push notification
            IndexRequest request = converter.convert(body, INDEX_ENV_VAR);
            vidId = request.id();
            System.out.println("Got " + vidId);

            client.index(request, RequestOptions.DEFAULT);
        } catch (Exception exception) {
            System.out.println("Failed to submit request");
            exception.printStackTrace();
        }
        return vidId;
    }

    //update elasticsearch with snippets and stats
    public void update() {
        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
        BulkRequest request = new BulkRequest();
        try {
            Update u = new Update();

            SearchRequest searchRequest = new SearchRequest(INDEX_ENV_VAR);
            searchRequest.scroll(scroll);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            //get all videos in index
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            searchRequest.source(searchSourceBuilder);

            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            String scrollId = searchResponse.getScrollId();
            SearchHit[] searchHits = searchResponse.getHits().getHits();

            int nUpdates = 0;
            System.out.println("Max updates: " + String.valueOf(MAX_UPDATES));
            while (searchHits != null && searchHits.length > 0 && nUpdates <= MAX_UPDATES) {

                for (SearchHit hit : searchHits) {
                    Map<String, Object> jsonMap = hit.getSourceAsMap();
                    String id = hit.getId();

                    //if no description, update snippets
                    if (!jsonMap.containsKey("description")) {
                        request.add(new UpdateRequest(INDEX_ENV_VAR, id)
                            .doc(u.updateSnippet(id))
                        );
                        nUpdates++;
                    }
                    //if no like count, update statistics
                    if (!jsonMap.containsKey("like_count")) {
                        request.add(new UpdateRequest(INDEX_ENV_VAR, id)
                            .doc(u.updateStats(id))
                        );
                        nUpdates++;
                    }
                    if (nUpdates > MAX_UPDATES) break;
                }

                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(scroll);
                searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();
            }

            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);

        } catch (Exception exception) {
            System.out.println("Failed to get search hits");
            exception.printStackTrace();
        }

        try {
            client.bulk(request, RequestOptions.DEFAULT);
        } catch (Exception exception) {
            System.out.println("Failed to submit update");
            exception.printStackTrace();
        }
    }

    //close the server
    public void close() throws IOException {
        try{
            client.close();
        } catch (IOException exception) {
            System.out.println("Failed to close Elasticsearch");
            exception.printStackTrace();
        }
    }
}
