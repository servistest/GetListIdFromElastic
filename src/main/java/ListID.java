import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class ListID {

    private static final String SOURCE_HOST = "hdcs-analytics-search-elastic.int.thomsonreuters.com";
    private static final String TARGET_HOST = "alpha-analyticssearch-search-elastic.int.thomsonreuters.com";
    private static final int SOURCE_PORT_ONE = 9200;
    private static final int TARGET_PORT_ONE = 9200;
    private static final int SOURCE_PORT_TWO = 9201;
    private static final int TARGET_PORT_TWO = 9201;
    private static final String SOURCE_SCHEME = "http";
    private static final String TARGET_SCHEME = "http";

    private static RestHighLevelClient restHighLevelClient;

    private static final String SOURCE_INDEX = "eikonsearch_orgs_1.121.0";
    private static final String TARGET_INDEX = "cat_pc_orgs_0.0.3";
    private static final String SOURCE_TYPE = "orgs";
    private static final String TARGET_TYPE = "orgs";
    private static final RequestOptions COMMON_OPTIONS;
    private static final Integer COUNT_OF_DOCUMENTS = 5;
    private static final Integer PRIMARY_SET_MULTIPLIER=2;
    public  final Boolean check = false;
    int k = 0;

//   + 1 Удалить лишние id в списке что бы было ровно 200
//    2. открытие конекшинов слшком много - убрать лишнее
//    3. попробывать ассихронный запрос на поиск Id   Очень долго обрабатывается.
//       https://www.elastic.co/guide/en/elasticsearch/client/java-rest/7.1/java-rest-low-usage-requests.html#java-rest-low-usage-request-options
//    4. сделать выгрузку в файл id

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

        List<String> listId= new ArrayList<>();
        Integer countId = COUNT_OF_DOCUMENTS*PRIMARY_SET_MULTIPLIER;

        while (listId.size()< COUNT_OF_DOCUMENTS) {
              listId= cheksDocInTargetIndex(getListIdFromSourceIndex(countId));
              countId=countId+(COUNT_OF_DOCUMENTS-listId.size())*2;
        }

        System.out.println("Target list of ID --> ");
        // we take only first COUNT_OF_DOCUMENTS

        listId.subList(0,COUNT_OF_DOCUMENTS).forEach(x -> System.out.print(" " +x));
//        listId.forEach(x -> System.out.print(" " +x));
        System.out.println();
    }

    private static List<String> getListIdFromSourceIndex(Integer countOfId) {
        System.out.print("Getting documents from source index -->...");
        makeConnection(SOURCE_HOST,SOURCE_PORT_ONE,SOURCE_PORT_TWO,SOURCE_SCHEME);
        SearchSourceBuilder sourceBuilder=   new SearchSourceBuilder();
        sourceBuilder.size(countOfId);
        // get only service fields
        sourceBuilder.fetchSource(false);

        MatchAllQueryBuilder matchAllQueryBuilder = new MatchAllQueryBuilder();

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(SOURCE_INDEX);
        searchRequest.setBatchedReduceSize(500);
        searchRequest.source(sourceBuilder.query(matchAllQueryBuilder));

        SearchResponse searchResponse = null;

        try {
            searchResponse = restHighLevelClient.search(searchRequest,COMMON_OPTIONS);
        } catch (IOException e) {
            e.printStackTrace();
        }

        List<String> listID = new ArrayList<>();

        if (searchResponse!= null) {

            searchResponse.getHits().forEach( x -> System.out.print(" " + x.getId()));
            System.out.println();
            searchResponse.getHits().forEach( x -> listID.add(x.getId()));
        }

        try {
            closeConnection();
        } catch (IOException e) {
            e.getLocalizedMessage();
        }
        return listID;
    }


    private static List<String> cheksDocInTargetIndex(List<String> listId) {



        makeConnection(TARGET_HOST,TARGET_PORT_ONE,TARGET_PORT_TWO,TARGET_SCHEME);

        List<String> existListId = new ArrayList<>();

        for (String id : listId){
            if(checksDocById(id)) {
                existListId.add(id);
            }
        }

        try {
            closeConnection();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return  existListId;

    }


    private static Boolean checksDocById(String id) {

        final boolean[] res = new boolean[1];
        final boolean temp  ;

        GetRequest getRequest = new GetRequest(
                TARGET_INDEX,
                TARGET_TYPE,
                id);

//        Disable fetching _source.
        getRequest.fetchSourceContext(new FetchSourceContext(false));

//        Disable fetching stored fields.
        getRequest.storedFields("_none_");

        boolean exists=true;
        ActionListener<Boolean> listener = new ActionListener<Boolean>() {
            @Override
            public void onResponse(Boolean aBoolean) {
                System.out.println("id = " + id  +" aBoolean = " + aBoolean.booleanValue());
                res[0] = aBoolean;
            }


            @Override
            public void onFailure(Exception e) {
                e.printStackTrace();
//                exists = false;
            }
        };
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


//             restHighLevelClient.getAsync(getRequest, COMMON_OPTIONS,listener);
        try {
            exists = restHighLevelClient.exists(getRequest, COMMON_OPTIONS);
        } catch (IOException e) {
            e.printStackTrace();
        }

//         restHighLevelClient.existsAsync(getRequest, COMMON_OPTIONS,listener);


        return  exists;
    }

    private static RestHighLevelClient makeConnection(String host, int portOne,int portTwo,String scheme) {

        if (restHighLevelClient == null) {
            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost(host, portOne, scheme),
                            new HttpHost(host, portTwo, scheme)));
        }
        return restHighLevelClient;
    }

    private static synchronized void closeConnection() throws IOException {
        restHighLevelClient.close();
        restHighLevelClient = null;
    }


}
