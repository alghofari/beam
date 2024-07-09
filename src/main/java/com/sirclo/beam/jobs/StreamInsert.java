package com.sirclo.beam.jobs;

import static com.sirclo.beam.transforms.StreamInsertTransforms.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sirclo.beam.options.StreamInsertOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.runners.spark.SparkRunner;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.List;
import java.util.Objects;

public class StreamInsert {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    public static class JsonFilter extends SimpleFunction<String, String> {
        @Override
        public String apply(String input) {
            try {

                JsonNode jsonNode = objectMapper.readTree(input);

                // Apply your filtering conditions here.
                if (jsonNode.has("action") && jsonNode.has("url")) {
                    String action = jsonNode.get("action").asText();
                    String url = jsonNode.get("url").asText();

                    if ("interior_fetch_order_detail_pubsub_success".equals(action) && "order-fetched".equals(url)) {
                        return input;
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
        String subscriptionId = "projects/" + projectId + "/subscriptions/" + subscriptionName;
        String tableName = options.getTableName();
        String username = options.getUsername();
        String password = options.getPassword();
        String jdbcUrl = options.getJdbcUrl();

        pipeline
                .apply("Read from Pub/Sub", PubsubIO.readStrings().fromSubscription(subscriptionId))
                .apply("Filter messages", MapElements
                        .into(TypeDescriptors.strings())
                        .via((SerializableFunction<String, String>) new JsonFilter()))
                .apply("Remove empty messages", Filter.by((String message) -> !Objects.equals(message, "")))
                .apply("Get Request Body", MapElements
                        .into(TypeDescriptors.strings())
                        .via((String jsonString) -> {
                            try {
                                return getRequestBody(jsonString);
                            } catch (Exception e) {
                                throw new RuntimeException("Error getting request body: " + e.getMessage(), e);
                            }
                        }))
                .apply("Transform Message", MapElements
                        .into(TypeDescriptors.lists(TypeDescriptors.strings()))
                        .via((String jsonString) -> {
                            try {
                                return transformMessage(jsonString);
                            } catch (Exception e) {
                                throw new RuntimeException("Error transforming message: " + e.getMessage(), e);
                            }
                        }))
                .apply("Execute SQL Queries", JdbcIO.<List<String>>write()
                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                                        "com.mysql.cj.jdbc.Driver", jdbcUrl)
                                .withUsername(username)
                                .withPassword(password))
                        .withStatement(String.join("INSERT INTO ", tableName, " (load_timestamp, id, brand_id, shop_name, order_date, customer_reference, order_status, paid_at, channel_code, remote_order_id, remote_warehouse_id, product_code, product_description, raw_price, quantity, line_total, discount_amount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"))
                        .withPreparedStatementSetter((List<String> listJson, PreparedStatement preparedStatement) -> {
                            for (String stringJson : Objects.requireNonNull(listJson)) {
                                JsonNode json = objectMapper.readTree(stringJson);
                                // Assuming you have a function to parse JSON and extract values

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
                                // ... Set other parameters ...

                                preparedStatement.addBatch();  // Add to batch for batch processing
                            }
                        })
                        .withBatchSize(100));
    }

    public static void main(String[] args) {
        // Set up pipeline options.
        PipelineOptionsFactory.register(StreamInsertOptions.class);
        StreamInsertOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(StreamInsertOptions.class);

        // Specify SparkRunner as the pipeline runner
        options.setRunner(SparkRunner.class);

        // Create the pipeline.
        Pipeline pipeline = Pipeline.create(options);
        StreamInsert.buildPipeline(pipeline, options);
        pipeline.run().waitUntilFinish();
    }
}
