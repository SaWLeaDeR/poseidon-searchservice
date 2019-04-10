package com.poseidon.service;

import com.amazonaws.*;
import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.http.ExecutionContext;
import com.amazonaws.http.HttpMethodName;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.discovery.converters.Auto;
import com.poseidon.config.ConfigurationInfo;
import com.poseidon.config.ElasticSearchConstants;
import com.poseidon.handlers.AwsResponse;
import com.poseidon.handlers.ElasticSearchClientHandler;
import com.poseidon.repository.Product;
import com.poseidon.repository.ProductQuery;
import com.poseidon.util.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.http.HttpStatus;


import javax.inject.Inject;
import javax.inject.Named;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.*;


@Named
@EnableConfigurationProperties(ConfigurationInfo.class)
public class ElasticSearchService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchService.class);

    private static final AWSCredentials AWS_CREDENTIALS = new DefaultAWSCredentialsProviderChain().getCredentials();

    @Inject
    private ConfigurationInfo configurationInfo;



    /**
     * Sign the request to AWS ElasticSearch using the AWS4Signer
     *
     * @param request The Request
     */
    private void signRequest(Request request) {
        final String region = configurationInfo.getRegion();
        final String serviceName = configurationInfo.getServiceName();

        final AWS4Signer aws4Signer = new AWS4Signer();
        aws4Signer.setRegionName(region);
        aws4Signer.setServiceName(serviceName);
        aws4Signer.sign(request, AWS_CREDENTIALS);
    }

    /**
     *  Build the full URL, create request headers, and build Request object prior to signing the Request to send
     *  to AWS ElasticSearch
     *
     * @param url The URL
     * @param json The request body
     * @param parameters The request parameters
     * @param httpMethodName The HTTPMethodName
     * @return The Request
     */
    private Request generateSignedRequest(final String url,
                                          final String json,
                                          final Map<String, List<String>> parameters,
                                          final HttpMethodName httpMethodName) {

        final String endpoint = configurationInfo.getEndpoint() + "/" + url;
        final Map<String, String> headers = new HashMap<>();
        headers.put("content-type", "application/json");

        final Request request = new DefaultRequest(configurationInfo.getServiceName());
        request.setHeaders(headers);

        // JSON is used for Creating and Updating objects in ElasticSearch
        if (json != null) {
            request.setContent(new ByteArrayInputStream(json.getBytes()));
        }
        // Parameters are used for queries
        if (parameters != null) {
            request.setParameters(parameters);
        }
        request.setEndpoint(URI.create(endpoint));
        request.setHttpMethod(httpMethodName);

        signRequest(request);

        return request;
    }

    /**
     * Submit the Request to AWS, and return the response
     *
     * @param request The Request
     * @return AwsResponse
     */
    private AwsResponse executeRequest(Request request) {
        try {
            final ClientConfiguration configuration = new ClientConfiguration();
            final ExecutionContext context = new ExecutionContext(true);
            final ElasticSearchClientHandler client = new ElasticSearchClientHandler(configuration);

            return client.execute(context, request);
        } catch (Exception e) {
            LOGGER.error("Error executing ElasticSearch Request.", e);
        }
        return null;
    }

    /**
     * Create a new document in ElasticSearch with a given Index, Document Mapping, Document Body, and Document ID
     *
     * @param index The index to create the new document in
     * @param type The mapping used by the index
     * @param json The document
     * @param id The document ID
     * @return AwsResponse
     */
    private AwsResponse createDocument(final String index, final String type, final String json, final String id) {
        final String url = index + "/" + type + "/" + id;
        final Request request = generateSignedRequest(url, json, null, HttpMethodName.PUT);

        return executeRequest(request);
    }
    /**
     * Delete a document from ElasticSearch with a given Index, Document Mapping, and Document ID
     *
     * @param index The index to delete the document from
     * @param type The mapping use by the index
     * @param id The ID of the document to be deleted
     * @return AwsResponse
     */
    public AwsResponse deleteDocument(final String index, final String type, final String id){
        final String url = index + "/" + type + "/" + id;
        // JSON and URL Parameters are not needed when deleting documents from ElasticSearch
        final Request request = generateSignedRequest(url, null, null, HttpMethodName.DELETE);

        return executeRequest(request);
    }

    /**
     * Create the Product in ElasticSearch
     *
     * @param product The Product
     * @return The response string
     * @throws JsonProcessingException Throws JsonProcessingException when response cannot be parsed
     */
    public String createNewProduct(Product product) throws JsonProcessingException {
        final ObjectMapper objectMapper = new ObjectMapper();
        final String json = objectMapper.writeValueAsString(product);
        if (json != null) {
            AwsResponse response = createDocument(ElasticSearchConstants.PRODUCTS_INDEX,
                ElasticSearchConstants.PRODUCTS_DOCUMENT_TYPE,
                json,
                product.getPartNo());
            // Creating a new document not seen before results in a 201 status, where as overwriting a previous document results in a 200
            if (response != null && (response.getHttpResponse().getStatusCode() == HttpStatus.CREATED.value()
                || response.getHttpResponse().getStatusCode() == HttpStatus.OK.value())) {
                LOGGER.info("Successfully created new Product", product.getPartNo(), product.getLevelOne());
                return product.getPartNo();
            }
        }

        return null;
    }

    /**
     * Generate the request from the API criteria, and return the results from ElasticSearch
     *
     * @param from Beginning point of the query
     * @param size Number of objects to return in the query
     * @param filterValues Optional list of values to filter the response by
     * @return Response
     */
    public String getProducts(final String index, final int from, final int size, Set<String> filterValues, final ProductQuery productQuery) {
        JSONObject query = new JSONObject();
        JSONObject bool = new JSONObject();
        JSONObject must = new JSONObject();
        JSONArray array = new JSONArray();

        createProductQuery(productQuery, array);

        query.put("from", from);
        query.put("size", size);
        must.put("must", array);
        bool.put("bool", must);
        query.put("query", bool);
        if (filterValues != null) {
            query.put("_source", filterValues);
        }

        final Map<String, List<String>> parameters = new HashMap<>();
        parameters.put(ElasticSearchConstants.FILTER_PATH, Collections.singletonList(ElasticSearchConstants.FILTER));

        final String url = index + ElasticSearchConstants.SEARCH_API;
        LOGGER.info("ES Query Body: {}", query);
        final Request request = generateSignedRequest(url, query.toString(), parameters, HttpMethodName.GET);

        final AwsResponse response = executeRequest(request);

        return response != null ? response.getBody() : "";
    }

    /**
     * Build a fuzzy search ElasticSearch query
     *
     * @param from Beginning point of the query
     * @param size Number of objects to return in the query
     * @param filterValues Optional list of values to filter the response by
     * @return Response
     */
    public String getProductsFuzzySearch(final String index, final int from, final int size, Set<String> filterValues, final ProductQuery productQuery) {
        JSONObject query = new JSONObject();
        JSONObject fuzzy = new JSONObject();
        JSONObject searchTerm = new JSONObject();

        createProductQueryFuzzySearch(productQuery, searchTerm);

        query.put("from", from);
        query.put("size", size);
        fuzzy.put("fuzzy", searchTerm);
        query.put("query", fuzzy);
        if (filterValues != null) {
            query.put("_source", filterValues);
        }

        final Map<String, List<String>> parameters = new HashMap<>();
        parameters.put(ElasticSearchConstants.FILTER_PATH, Collections.singletonList(ElasticSearchConstants.FILTER));

        final String url = index + ElasticSearchConstants.SEARCH_API;
        LOGGER.info("ES Query Body: {}", query);
        final Request request = generateSignedRequest(url, query.toString(), parameters, HttpMethodName.GET);

        final AwsResponse response = executeRequest(request);

        return response != null ? response.getBody() : "";
    }

    /**
     * Build a query statement from the MovieQuery object
     *
     * @param productQuery The ProductQuery
     * @param array The JSONArray
     */
    private void createProductQuery(final ProductQuery productQuery, JSONArray array) {
      if(productQuery.getPartNo() != null){
        buildElasticSearchMatchStatement("partNo", productQuery.getPartNo(), array);
      }
    }

    /**
     * Build an ElasticSearch fuzzy search statement.
     *
     * @param productQuery The MovieQuery
     * @param searchTerm The term to search for
     */
    private void createProductQueryFuzzySearch(final ProductQuery productQuery, JSONObject searchTerm) {
        if(StringUtils.checkNullOrEmpty(productQuery.getLevelOne())){
            buildElasticSearchFuzzyStatement("levelOne", productQuery.getLevelOne(), searchTerm);
        }

    }




    /**
     * Build an ElasticSearch 'should' statement.
     *
     * @param field The field to search in
     * @param value The value to search for
     * @param array The JSONArray to append the the query to
     */
    private void buildElasticSearchShouldStatement(final String field, final Collection value,
                                                   final JSONArray array) {
        if (value.size() > 1) {
            final JSONObject bool = new JSONObject();
            final JSONObject should = new JSONObject();
            final JSONArray match = new JSONArray();
            for (Object objectValue : value) {
                buildElasticSearchMatchStatement(field, objectValue, match);
            }
            should.put("should", match);
            bool.put("bool", should);
            array.put(bool);
        } else {
            buildElasticSearchMatchStatement(field, value.iterator().next(), array);
        }
    }

    /**
     * Build an ElasticSearch 'match' statement. This is equivalent to a SQL 'equals' statement.
     *
     * @param field The field to search in
     * @param value The value to search for
     * @param array The JSONArray to append the query to
     */
    private void buildElasticSearchMatchStatement(final String field, final Object value, final JSONArray array) {
        final JSONObject matchItem = new JSONObject();
        final JSONObject matchTerms = new JSONObject();
        matchTerms.put(field, value);
        matchItem.put("match", matchTerms);
        array.put(matchItem);
    }

    /**
     * Build a fuzzy search clause
     *
     * @param field The field to search in
     * @param value The partial value to search for
     * @param searchTerm The JSONObject
     */
    private void buildElasticSearchFuzzyStatement(final String field, final Object value, final JSONObject searchTerm) {
        final JSONObject fuzzyBlock = new JSONObject();
        fuzzyBlock.put("value", value);
        fuzzyBlock.put("boost", 1.0);
        fuzzyBlock.put("fuzziness", 50);
        fuzzyBlock.put("prefix_length", 0);
        fuzzyBlock.put("max_expansions", 100);
        searchTerm.put(field, fuzzyBlock);
    }

    /**
     * Build request to /_stats API in ElasticSearch
     *
     * @param index The Index
     * @return Response
     */
    public String getIndexStatistics(final String index) {
        final String url = index + ElasticSearchConstants.STATS_API;

        final Request request = generateSignedRequest(url, null, null, HttpMethodName.GET);

        final AwsResponse response = executeRequest(request);

        return response != null ? response.getBody() : "";
    }

}
