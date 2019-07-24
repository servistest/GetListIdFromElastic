import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

@JsonIgnoreProperties(ignoreUnknown = true)

public class App {

    private static final String HOST = "alpha-analyticssearch-search-elastic.int.thomsonreuters.com";
    private static final int PORT_ONE = 9200;
    private static final int PORT_TWO = 9201;
    private static final String SCHEME = "http";

    private static RestHighLevelClient restHighLevelClient;
    private static final ObjectMapper objectMapper =
            new ObjectMapper()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static final String INDEX = "cat_pc_orgs_0.0.3";
    private static final String TYPE = "orgs";
    private static final RequestOptions COMMON_OPTIONS;

    static {
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.
                setHttpAsyncResponseConsumerFactory(
                        new HttpAsyncResponseConsumerFactory
                                .HeapBufferedResponseConsumerFactory(30 * 1024 * 1024)

                );
        COMMON_OPTIONS = builder.build();
    }

    public static void main(String[] args) {
        getResult();
    }

    private static void getResult() {
        System.out.println("Getting document...");
        makeConnection();
        String id = "5000734792";
        Orgs docFromES = getORGSById(id);
        System.out.println("Person from DB  --> " + docFromES);


        try {
            closeConnection();
        } catch (IOException e) {
            e.getLocalizedMessage();
        }
    }

    private static Orgs getORGSById(String id) {

        GetRequest getPersonRequest = new GetRequest(
                INDEX,
                TYPE,
                id);
        GetResponse getResponse = null;

        try {
            getResponse = restHighLevelClient.get(getPersonRequest, COMMON_OPTIONS);
        } catch (java.io.IOException e) {
            e.getLocalizedMessage();
        }

        Orgs orgs = new Orgs();
        if (getResponse != null) {
            orgs = objectMapper.convertValue(getResponse.getSourceAsMap(), Orgs.class);
        }


        return orgs.setDocID(id);

        //        System.out.println("Fields = " + getResponse.getFields());
//        System.out.println("id = " + getResponse.getId());
//        System.out.println("Source = " +getResponse.getSource());

//        return
//                new Orgs()
//                .setCommonName(getResponse.getSource().get("CommonName").toString())
//                .setCommonName(getResponse.getSource().get("DocumentTitle").toString())
//                .setDocID(getResponse.getId());

    }



    private static void makeConnection() {

        if (restHighLevelClient == null) {
            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost(HOST, PORT_ONE, SCHEME),
                            new HttpHost(HOST, PORT_TWO, SCHEME)));
        }
    }

    private static synchronized void closeConnection() throws IOException {
        restHighLevelClient.close();
        restHighLevelClient = null;
    }


}
