package com.sirclo.beam.transforms;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class StreamInsertTransforms {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    public static String getRequestBody(String input) throws Exception {
        JsonNode rootNode = objectMapper.readTree(input);
//        return objectMapper.readTree(requestBody);
        return rootNode.get("request_body").asText();
    }

    public static List<String> transformMessage(String input) throws Exception {
        JsonNode requestBodyNode = objectMapper.readTree(input);
        JsonNode order = requestBodyNode.get("order");
        ArrayNode lineItems = (ArrayNode) order.get("line_items");
        String formattedTimestamp = formatTimestamp(LocalDateTime.now());
        String orderDate = order.get("order_date").asText();
        String formattedOrderDate = formatDate(orderDate);
        String paidAt = order.get("paid_at").asText();
        String formattedPaidAt = formatDate(paidAt);
        List<String> listJson = new ArrayList<>();

        // Iterate over the array and create arrayJson
        for (JsonNode element : lineItems) {
            // Create a new ObjectNode for each row
            ObjectNode extractedJson = objectMapper.createObjectNode()
                    .put("load_timestamp", formattedTimestamp)
                    .put("id", requestBodyNode.get("id").asText().replace("'", "''"))
                    .put("brand_id", requestBodyNode.get("brand_id").asText().replace("'", "''"))
                    .put("shop_name", requestBodyNode.get("shop_name").asText().replace("'", "''"))
                    .put("order_date", formattedOrderDate)
                    .put("customer_reference", order.get("customer_reference").asText().replace("'", "''"))
                    .put("order_status", order.get("order_status").asText().replace("'", "''"))
                    .put("paid_at", formattedPaidAt)
                    .put("channel_code", order.get("channel_code").asText().replace("'", "''"))
                    .put("remote_order_id", order.get("remote_order_id").asText().replace("'", "''"))
                    .put("remote_warehouse_id", order.get("remote_warehouse_id").asText().replace("'", "''"))
                    .put("product_code", element.get("product_code").asText().replace("'", "''"))
                    .put("product_description", element.get("product_description").asText().replace("'", "''"))
                    .put("raw_price", element.get("raw_price").asDouble())
                    .put("quantity", element.get("quantity").asInt())
                    .put("line_total", element.get("line_total").asDouble())
                    .put("discount_amount", element.get("discount_amount").asDouble());

            // Add the row to the array
            String jsonString = objectMapper.writeValueAsString(extractedJson);
            listJson.add(jsonString);
//            arrayJson.add(extractedJson);
        }
        return listJson;
    }

    private static String formatTimestamp(LocalDateTime timestamp) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return timestamp.format(formatter);
    }

    private static String formatDate(String orderDate) {
        // Check if the order date is "0001-01-01T00:00:00Z"
        if ("0001-01-01T00:00:00Z".equals(orderDate)) {
            return null;
        }

        // Parse the input order date string
        ZonedDateTime zonedDateTime = ZonedDateTime.parse(orderDate);

        // Format the order date
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return zonedDateTime.format(formatter);
    }

    private static String buildInsertQuery(String tableName, String input) throws JsonProcessingException {
        JsonNode jsonData = objectMapper.readTree(input);
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("INSERT INTO ").append(tableName).append(" (");

        // Append column names
        Iterator<String> fieldNames = jsonData.fieldNames();
        while (fieldNames.hasNext()) {
            queryBuilder.append(fieldNames.next()).append(", ");
        }
        queryBuilder.delete(queryBuilder.length() - 2, queryBuilder.length()); // Remove the last comma and space
        queryBuilder.append(") VALUES (");

        // Append values
        Iterator<JsonNode> values = jsonData.elements();
        while (values.hasNext()) {
            JsonNode value = values.next();
            if (value.isTextual()) {
                queryBuilder.append("'").append(value.textValue()).append("', ");
            } else {
                queryBuilder.append(value).append(", ");
            }
        }
        queryBuilder.delete(queryBuilder.length() - 2, queryBuilder.length()); // Remove the last comma and space
        queryBuilder.append(");");

        return queryBuilder.toString();
    }

    public static void executeInsertQuery(String url, String username, String password, String insertQuery) {
        // Clean non ASCII characters inside query.
        String cleanedQuery = insertQuery.replaceAll("[^\\x00-\\x7F]", "");

        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            // Disable auto-commit to manage transactions
            connection.setAutoCommit(false);

            try (Statement statement = connection.createStatement()) {
                //Execute query.
                statement.executeUpdate(cleanedQuery);

                // Commit the transaction
                connection.commit();

                // Print success message
                System.out.println("Insert query executed successfully.");
            } catch (Exception e) {
                // Handle exceptions related to statement execution
                e.printStackTrace();
                // Rollback the transaction in case of an error
                connection.rollback();
                // Print error message
                System.err.println("Error executing insert query " + insertQuery + " with details of error:" + e.getMessage());
            }
        } catch (SQLException e) {
            // Handle exceptions related to connection setup
            e.printStackTrace();
            // Print error message
            System.err.println("Error establishing database connection: " + e.getMessage());
        }
    }

    public static String createAndInsertQueries(List<String> arrayJson, String tableName, String url, String username, String password) throws JsonProcessingException {
        List<String> insertQueries = new ArrayList<>();
        for (String data : arrayJson) {
            String query = buildInsertQuery(tableName, data);
            String cleanedQuery = query.replaceAll("[^\\x00-\\x7F]", "");
            insertQueries.add(cleanedQuery);
        }

        for (String query : insertQueries){
            executeInsertQuery(url, username, password, query);
        }
        return "Pipeline is done.";
    }
}
