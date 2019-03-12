package com.kafka.experiments.consumer;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;


/**
 * Static Configuration set up
 */
public class ElasticSearcheRestClientConnection {

    public static RestHighLevelClient createClient() {

        //  String hostname = "localhost";
        //  RestClientBuilder builder = RestClient.builder(new HttpHost(hostname,9200,"http"));

        // replace with your own credentials
        String hostname = "localhost or bonsai url"; // localhost or bonsai url
        String username = "username if you have"; // needed only for bonsai, not needed for local elastic search
        String password = "password if you have"; // needed only for bonsai , not needed for local elastic search

        // credentials provider help supply username and password
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }
}
