package com.poseidon.config;

/**
 * Application constants.
 */
public final class ElasticSearchConstants {

    // Regex for acceptable logins
    public static final String LOGIN_REGEX = "^[_.@A-Za-z0-9-]*$";

    public static final String SYSTEM_ACCOUNT = "system";
    public static final String ANONYMOUS_USER = "anonymoususer";
    public static final String DEFAULT_LANGUAGE = "en";

    //ES
    public static final String PRODUCTS_INDEX = "products";
    public static final String PRODUCTS_DOCUMENT_TYPE = "product";

    public static final String FILTER_PATH = "filter_path";
    public static final String FILTER = "hits.hits._source";
    public static final String SEARCH_API = "/_search";
    public static final String STATS_API = "/_stats";

    public static final String EMPTY_RESPONSE = "{}";
    
    private ElasticSearchConstants() {
    }
}
