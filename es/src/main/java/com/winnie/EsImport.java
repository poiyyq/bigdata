package com.winnie;

import com.winnie.util.ExcelUtils;
import org.apache.http.HttpHost;
import org.apache.logging.log4j.util.PropertiesUtil;
import org.apache.poi.hssf.usermodel.HSSFWorkbookFactory;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.bytes.BytesReference;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * 将数据导入es
 */
public class EsImport {
    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.load(EsImport.class.getResourceAsStream("/esconfig.properties"));
        String hostAndPorts = props.getProperty("hostAndPorts");
        List<HttpHost> httpHostList = new ArrayList<>();
        if(hostAndPorts!=null && !hostAndPorts.equals("")){
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

        // 加载数据
        String dataPath = "/Users/admin/Downloads/data.xlsx";
        List<Map<String, Object>> datas = ExcelUtils.getData(dataPath);
        datas.forEach(data->{
            String index = "emp";
            String type = "_doc";
            IndexRequest indexRequest = new IndexRequest(index, type).source(data);
            // 同步执行
            try {
                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        client.close();
    }
}
