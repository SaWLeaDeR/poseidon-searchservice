package com.poseidon.web.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.poseidon.config.ElasticSearchConstants;
import com.poseidon.repository.Product;
import com.poseidon.repository.ProductQuery;
import com.poseidon.service.ElasticSearchService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.inject.Inject;

@RestController
@RequestMapping("/api")
public class ElasticSearchResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchResource.class);

    @Inject
    private ElasticSearchService elasticSearchService;

    /**
     * Create a new Product in ElasticSearch
     *
     * @param product The Product object
     * @return Response Entity
     */
    @PostMapping(value = "/create", produces = {MediaType.TEXT_PLAIN_VALUE})
    @ResponseBody
    public ResponseEntity<String> createElasticSearchObject(@RequestBody final Product product) {
        String title;
        try {
            title = elasticSearchService.createNewProduct(product);
            if (title != null) {
                return ResponseEntity.status(HttpStatus.OK).body("Successfully created " + title);
            }
        } catch (JsonProcessingException e) {
            LOGGER.error("Failed to create Product.", e);
        }
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to create  " + product.getPartNo());
    }

    /**
     * Get a Set of Movies that match your query criteria
     *
     * @param productQuery The query
     * @return Set of Movies
     */
    @PostMapping(value = "/search", produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public ResponseEntity<String> getFromElasticSearch(@RequestBody final ProductQuery productQuery) {
        return ResponseEntity.status(HttpStatus.OK).body(
            elasticSearchService.getProducts(ElasticSearchConstants.PRODUCTS_INDEX, 0, 100, null, productQuery));
    }

    /**
     * Fuzzy search the Products index with a partial word, or one word in a sentence.
     *
     * @param productQuery The query
     * @return Set of Movies
     */
    @PostMapping(value = "/fuzzySearch", produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public ResponseEntity<String> getFromElasticSearchFuzzySearch(@RequestBody final ProductQuery productQuery) {
        return ResponseEntity.status(HttpStatus.OK).body(
            elasticSearchService.getProductsFuzzySearch(ElasticSearchConstants.PRODUCTS_INDEX, 0, 100, null, productQuery));
    }

    /**
     * Get statistics about an ElasticSearch Index
     *
     * @param index The targeted index
     * @return Response Entity
     */
    @GetMapping(value = "/statistics", produces = {MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public ResponseEntity<String> indexStatistics(@RequestParam("index") final String index) {
        String response = elasticSearchService.getIndexStatistics(index);
        if (response != null) {
            return ResponseEntity.status(HttpStatus.OK).body(response);
        } else {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error fetching statistics for index");
        }
    }
}
