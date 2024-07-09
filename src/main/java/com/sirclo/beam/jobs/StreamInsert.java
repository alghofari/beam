package com.sirclo.beam.jobs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sirclo.beam.options.StreamInsertOptions;
import com.sirclo.beam.transforms.StreamInsertTransforms;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.io.jdbc.JdbcIO;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.sirclo.beam.transforms.StreamInsertTransforms.*;

public class StreamInsert {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger LOG = Logger.getLogger(StreamInsertTransforms.class.getName());

    private static String concatSubscriptionId(String projectId, String subscriptionName) {
        return "projects/" + projectId + "/subscriptions/" + subscriptionName;
    }

    private static String readQueryFromFile(String filePath) {
        try {
            InputStream inputStream = Objects.requireNonNull(StreamInsert.class.getClassLoader().getResourceAsStream(filePath));
            return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            // Handle exceptions here, e.g., log or throw a custom exception
            LOG.log(Level.SEVERE, "Error reading query from: " + filePath, e);
            return null; // Or throw an exception based on your needs
        }
    }

    public static class JsonFilter extends SimpleFunction<String, String> {
        @Override
        public String apply(String input) {
            try {

                JsonNode jsonNode = objectMapper.readTree(input);

                // Apply your filtering conditions here.
                if (jsonNode.has("action") && jsonNode.has("url")) {
                    String action = jsonNode.get("action").asText();
                    String url = jsonNode.get("url").asText();
                    String channel = jsonNode.get("channel").asText();
                    String remoteOrderStatus = jsonNode.get("remote_order_status").asText();

                    if ("interior_fetch_order_detail_pubsub_success".equals(action) && "order-fetched".equals(url)) {
                        if (isFirstStatus(channel, remoteOrderStatus)) {
                            return jsonNode.get("request_body").asText();
                        }
                    }
                }
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }

            return ""; // Return empty string for messages that do not meet the filtering conditions.
        }
    }

    public static void buildPipeline(Pipeline pipeline, StreamInsertOptions options) {
        String projectId = options.getProjectId();
        String subscriptionName = options.getSubscriptionName();
        String tableName = options.getTableName();
        String username = options.getUsername();
        String password = options.getPassword();
        String jdbcUrl = options.getJdbcUrl();
        Integer batchSize = options.getBatchSize();
        String subscriptionId = concatSubscriptionId(projectId, subscriptionName);
        String queryInsert = readQueryFromFile("query/insert_update_query.sql");
        String queryMasterStore = readQueryFromFile("query/master_store.sql");
        String queryMasterMarketplace = readQueryFromFile("query/master_marketplace.sql");

        PCollectionView<Map<String, String>> masterStore = createMapViewFromQuery(
                pipeline,
                jdbcUrl,
                username,
                password,
                queryMasterStore,
                "brand_id",
                "official_store",
                "As Master Store Map View");

        PCollectionView<Map<String, String>> masterMarketplace = createMapViewFromQuery(
                pipeline,
                jdbcUrl,
                username,
                password,
                queryMasterMarketplace,
                "mp_code",
                "marketplace",
                "As Master Marketplace Map View");

        pipeline
                .apply("Read from Pub/Sub", PubsubIO.readStrings().fromSubscription(subscriptionId))
                .apply("Filter messages", MapElements
                        .into(TypeDescriptors.strings())
                        .via((SerializableFunction<String, String>) new JsonFilter()))
                .apply("Remove empty messages", Filter.by((String message) -> !Objects.equals(message, "")))
                .apply("Transform Message", MapElements
                        .into(TypeDescriptors.lists(TypeDescriptors.strings()))
                        .via((String jsonString) -> {
                            try {
                                return transformMessage(jsonString);
                            } catch (Exception e) {
                                throw new RuntimeException("Error transforming message: " + e.getMessage(), e);
                            }
                        }))
                .apply("Join with OS Oms and MP Oms Result", joinWithOmsChannel(masterStore, masterMarketplace))
                .apply("Execute SQL Queries", JdbcIO.<List<String>>write()
                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                                        "com.mysql.cj.jdbc.Driver", jdbcUrl)
                                .withUsername(username)
                                .withPassword(password))
                        .withStatement(String.format(queryInsert, tableName))
                        .withPreparedStatementSetter((List<String> listJson, PreparedStatement preparedStatement) -> {
                            assert listJson != null;
                            for (String stringJson : listJson) {
                                JsonNode json = objectMapper.readTree(stringJson);
                                try {
                                    // Set parameters based on your JSON structure
                                    preparedStatement.setTimestamp(1, Timestamp.valueOf(json.get("load_timestamp").asText())); // Assuming load_timestamp is a string in ISO format
                                    preparedStatement.setString(2, json.get("id").asText());
                                    preparedStatement.setString(3, json.get("brand_id").asText());
                                    preparedStatement.setString(4, json.get("shop_name").asText());
                                    preparedStatement.setString(5, json.get("order_date").asText());
                                    preparedStatement.setString(6, json.get("customer_reference").asText());
                                    preparedStatement.setString(7, json.get("order_status").asText());
                                    preparedStatement.setString(8, json.get("paid_at").asText());
                                    preparedStatement.setString(9, json.get("channel_code").asText());
                                    preparedStatement.setString(10, json.get("remote_order_id").asText());
                                    preparedStatement.setString(11, json.get("remote_warehouse_id").asText());
                                    preparedStatement.setString(12, json.get("product_code").asText());
                                    preparedStatement.setString(13, json.get("product_description").asText());
                                    preparedStatement.setDouble(14, json.get("raw_price").doubleValue());
                                    preparedStatement.setInt(15, json.get("quantity").intValue());
                                    preparedStatement.setDouble(16, json.get("line_total").doubleValue());
                                    preparedStatement.setDouble(17, json.get("discount_amount").doubleValue());
                                    preparedStatement.setString(18, json.get("marketplace").asText());
                                    preparedStatement.setString(19, json.get("remote_order_item_id").asText());
                                    preparedStatement.setString(20, json.get("line_number").asText());
                                    // ... Set other parameters ...

                                    preparedStatement.addBatch(); // Add to batch for batch processing
                                } catch (Exception e) {
                                // Log an error message and the exception stack trace
                                LOG.log(Level.SEVERE, "Error processing JSON for id: " + json.get("id").asText(), e);

                            }
                            }
                        })
                        .withBatchSize(batchSize));
    }

    public static void main(String[] args) {
        // Set up pipeline options.
        PipelineOptionsFactory.register(StreamInsertOptions.class);
        StreamInsertOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(StreamInsertOptions.class);

        // Create the pipeline.
        Pipeline pipeline = Pipeline.create(options);
        StreamInsert.buildPipeline(pipeline, options);
        pipeline.run().waitUntilFinish();
    }
}
