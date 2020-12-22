package com.winnie;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class EsSearch {
    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.load(EsImport.class.getResourceAsStream("/esconfig.properties"));
        String hostAndPorts = props.getProperty("hostAndPorts");
        List<HttpHost> httpHostList = new ArrayList<>();
        if (hostAndPorts != null && !hostAndPorts.equals("")) {
            String[] hostAndPortArr = hostAndPorts.split(",");
            for (String hostAndPort : hostAndPortArr) {
                String[] split = hostAndPort.split(":");
                String host = split[0];
                Integer port = Integer.parseInt(split[1]);
                httpHostList.add(new HttpHost(host, port));
            }
        }
        HttpHost[] httpHosts = new HttpHost[httpHostList.size()];
        for (int i = 0; i < httpHostList.size(); i++) {
            httpHosts[i] = httpHostList.get(i);
        }
        RestClientBuilder builder = RestClient.builder(httpHosts);
        RestHighLevelClient client = new RestHighLevelClient(builder);
        String index = "emp";
        String type = "_doc";
        SearchRequest searchRequest = new SearchRequest(index);
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = search.getHits();
        long totalHits = hits.getTotalHits();
        for (long i = 0; i < totalHits; i++) {
            SearchHit at = hits.getAt(Integer.parseInt(i + ""));
            Map<String, Object> sourceAsMap = at.getSourceAsMap();
            System.out.println(sourceAsMap.get("way_a_reason"));
        }
    }
}
