
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class MultiGetId {

    private static final String SOURCE_HOST = "hdcs-analytics-search-elastic.int.thomsonreuters.com";
    private static final String TARGET_HOST = "alpha-analyticssearch-search-elastic.int.thomsonreuters.com";
    private static final int SOURCE_PORT_ONE = 9200;
    private static final int TARGET_PORT_ONE = 9200;
    private static final int SOURCE_PORT_TWO = 9201;
    private static final int TARGET_PORT_TWO = 9201;
    private static final String SOURCE_SCHEME = "http";
    private static final String TARGET_SCHEME = "http";
    private static final String SOURCE_INDEX = "eikonsearch_orgs_1.121.0";
    private static final String TARGET_INDEX = "cat_pc_orgs_0.0.3";
    private static final String SOURCE_TYPE = "orgs";
    private static final String TARGET_TYPE = "orgs";
    private static final String FILE_NAME = "list_id.txt";
    private static final String FILE_ENCODING = "UTF-8";
    private static final RequestOptions COMMON_OPTIONS;
    private static final Integer COUNT_OF_DOCUMENTS = 200;
    private static final Integer PRIMARY_SET_MULTIPLIER = 1;
    private static RestHighLevelClient restHighLevelClientSource;
    private static RestHighLevelClient restHighLevelClientTarget;

    private  static  Logger logger = LoggerFactory.getLogger(MultiGetId.class);

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

        restHighLevelClientSource = makeConnection(SOURCE_HOST, SOURCE_PORT_ONE, SOURCE_PORT_TWO, SOURCE_SCHEME);
        restHighLevelClientTarget = makeConnection(TARGET_HOST, TARGET_PORT_ONE, TARGET_PORT_TWO, TARGET_SCHEME);

        saveIdToFileAndPrint(findIds());

        closeConnection(restHighLevelClientSource);
        closeConnection(restHighLevelClientTarget);
    }

    private static List<String> findIds() {

        List<String> listId = new ArrayList<>();
        Integer countId = COUNT_OF_DOCUMENTS * PRIMARY_SET_MULTIPLIER;
        while (listId.size() < COUNT_OF_DOCUMENTS) {
            listId = checkDocsInTargetIndex(getListIdFromSourceIndex(countId));
            countId = countId + (COUNT_OF_DOCUMENTS - listId.size()) * 2;
        }
        return listId;

    }

    private static void saveIdToFileAndPrint(List<String> listId) {

        logger.info("Target list of ID --> ");

        // we take only first COUNT_OF_DOCUMENTS
        String idCommaSeparated = String.join(",", listId.subList(0, COUNT_OF_DOCUMENTS));
        logger.info(idCommaSeparated);

        File file = new File(FILE_NAME);
        try {
            FileUtils.writeStringToFile(file, idCommaSeparated, FILE_ENCODING, false);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static SearchRequest createSearchRequestForSourceIndex(Integer countOfId) {

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(countOfId);
        // get only service fields
        sourceBuilder.fetchSource(false);

        MatchAllQueryBuilder matchAllQueryBuilder = new MatchAllQueryBuilder();

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(SOURCE_INDEX);
        searchRequest.setBatchedReduceSize(500);
        searchRequest.source(sourceBuilder.query(matchAllQueryBuilder));

        return searchRequest;
    }

    private static List<String> responseFromSourceIndexProcessing(SearchResponse searchResponse) {

        List<String> listId = new ArrayList<>();

        logger.info("Getting documents from source index -->...");
        if (searchResponse != null) {
            searchResponse.getHits().forEach(x -> {
                listId.add(x.getId());
            });
            logger.info(String.join(",", listId));
        }
        return listId;
    }

    private static List<String> getListIdFromSourceIndex(Integer countOfId) {

        SearchResponse searchResponse = null;

        try {
            searchResponse = restHighLevelClientSource.search(createSearchRequestForSourceIndex(countOfId), COMMON_OPTIONS);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return responseFromSourceIndexProcessing(searchResponse);
    }

    private static MultiGetRequest createRequestForTargetIndex(List<String> listId) {

        MultiGetRequest multiGetRequest = new MultiGetRequest();

        for (String id : listId) {
            multiGetRequest.add(new MultiGetRequest.Item(
                    TARGET_INDEX,
                    id
            ).fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE));
        }
        return multiGetRequest;
    }

    private static List<String> checkDocsInTargetIndex(List<String> listId) {

        MultiGetResponse multiGetResponse = null;

        try {
            multiGetResponse = restHighLevelClientTarget.mget(createRequestForTargetIndex(listId), COMMON_OPTIONS);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return ifFiltering(multiGetResponse);
    }


    private static List<String> ifFiltering(MultiGetResponse multiGetResponse) {

        List<String> existListId = new ArrayList<>();

        for (MultiGetItemResponse multiGetItemResponses : multiGetResponse.getResponses()) {
            if (multiGetItemResponses.getResponse().isExists()) {
                existListId.add(multiGetItemResponses.getId());
            }
        }
        return existListId;
    }

    private static RestHighLevelClient makeConnection(String host, int portOne, int portTwo, String scheme) {

        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(host, portOne, scheme),
                        new HttpHost(host, portTwo, scheme)));

        return restHighLevelClient;
    }

    private static synchronized void closeConnection(RestHighLevelClient restHighLevelClient) {

        try {
            restHighLevelClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}
