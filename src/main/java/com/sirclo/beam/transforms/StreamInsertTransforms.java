package com.sirclo.beam.transforms;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StreamInsertTransforms {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger LOG = Logger.getLogger(StreamInsertTransforms.class.getName());


    public static List<String> transformMessage(String input) throws Exception {
        try {
            JsonNode requestBodyNode = objectMapper.readTree(input);
            JsonNode order = requestBodyNode.get("order");
            String orderStatus = order.get("order_status").asText();
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
                        .put("order_status", orderStatus.replace("'", "''"))
                        .put("paid_at", formattedPaidAt)
                        .put("channel_code", order.get("channel_code").asText().replace("'", "''"))
                        .put("remote_order_id", order.get("remote_order_id").asText().replace("'", "''"))
                        .put("remote_warehouse_id", order.get("remote_warehouse_id").asText().replace("'", "''"))
                        .put("product_code", element.get("product_code").asText().replace("'", "''"))
                        .put("product_description", element.get("product_description").asText().replace("'", "''"))
                        .put("raw_price", element.get("raw_price").asDouble())
                        .put("quantity", element.get("quantity").asInt())
                        .put("line_total", element.get("line_total").asDouble())
                        .put("discount_amount", element.get("discount_amount").asDouble())
                        .put("remote_order_item_id", element.get("remote_order_item_id").asText().replace("'", "''"))
                        .put("line_number", element.get("quantity").asInt());

                // Add the row to the array
                String jsonString = objectMapper.writeValueAsString(extractedJson);
                listJson.add(jsonString);

            }

            return listJson;
        } catch (JsonProcessingException e) {
            // SEVERE: Log an error message and the exception stack trace for JsonProcessingException
            LOG.log(Level.SEVERE, "Error processing JSON: " + e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            // SEVERE: Log an error message and the exception stack trace for other exceptions
            LOG.log(Level.SEVERE, "An error occurred during message transformation: " + e.getMessage(), e);
            throw e;
        }
    }

    public static boolean isFirstStatus(String channel, String remoteOrderStatus) {
        return ("blib".equals(channel) && ("PU".equals(remoteOrderStatus) || "FP".equals(remoteOrderStatus))) ||
                ("lzda".equals(channel) && ("unpaid".equals(remoteOrderStatus) || "pending".equals(remoteOrderStatus) || "packed".equals(remoteOrderStatus))) ||
                ("shpe".equals(channel) && ("PROCESSED".equals(remoteOrderStatus) || "READY_TO_SHIP".equals(remoteOrderStatus) || "UNPAID".equals(remoteOrderStatus))) ||
                ("tkpd".equals(channel) && "220".equals(remoteOrderStatus)) ||
                ("tktk".equals(channel) && ("accepted".equals(remoteOrderStatus) || "packed".equals(remoteOrderStatus) || "unpaid".equals(remoteOrderStatus))) ||
                ("zlra".equals(channel) && "pending".equals(remoteOrderStatus));
    }


    private static String formatTimestamp(LocalDateTime timestamp) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return timestamp.format(formatter);
    }

    private static String formatDate(String orderDate) {
        // Check if the order date is "0001-01-01T00:00:00Z"
        if (orderDate == null || "null".equals(orderDate) || "0001-01-01T00:00:00Z".equals(orderDate)) {
            return "0000-01-01 00:00:00";
        }

        // Parse the input order date string
        ZonedDateTime zonedDateTime = ZonedDateTime.parse(orderDate);

        // Format the order date
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return zonedDateTime.format(formatter);
    }

    public static ParDo.SingleOutput<List<String>, List<String>> joinWithOmsChannel(
            PCollectionView<Map<String, String>> osOms,
            PCollectionView<Map<String, String>> mpOms) {

        return ParDo.of(new DoFn<List<String>, List<String>>() {
            @ProcessElement
            public void processElement(@Element List<String> listJson, ProcessContext c) throws JsonProcessingException {
                Map<String, String> osOmsMap = c.sideInput(osOms);
                Map<String, String> mpOmsMap = c.sideInput(mpOms);
                List<String> joinedJson = new ArrayList<>();

                for (String stringJson : listJson) {
                    JsonNode json = objectMapper.readTree(stringJson);
                    ObjectNode objectNode = (ObjectNode) json;
                    objectNode.putNull("marketplace");
                    ObjectNode joinedBrandId = updateValueFromJoin(objectNode, json.get("brand_id").asText(), osOmsMap, "shop_name");
                    ObjectNode joinedShopName = updateValueFromJoin(joinedBrandId, json.get("channel_code").asText(), mpOmsMap, "marketplace");
                    joinedJson.add(joinedShopName.toString());
                }
                c.output(joinedJson);
            }
        }).withSideInputs(osOms, mpOms);
    }

    public static ObjectNode updateValueFromJoin(ObjectNode objectNode, String keyName, Map<String, String> map, String updateKey) {
        String value = map.get(keyName);
        if (value != null) {
            objectNode.put(updateKey, value);
        }
        return objectNode;
    }

    public static PCollectionView<Map<String, String>> createMapViewFromQuery(
            Pipeline pipeline,
            String jdbcUrl,
            String username,
            String password,
            String query,
            String keyColumn,
            String valueColumn,
            String viewName) {

        return pipeline
                .apply("Read from JDBC", JdbcIO.<KV<String, String>>read()
                        .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                                        "com.mysql.cj.jdbc.Driver", jdbcUrl)
                                .withUsername(username)
                                .withPassword(password))
                        .withQuery(query)
                        .withCoder(KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()))
                        .withRowMapper((JdbcIO.RowMapper<KV<String, String>>) resultSet -> {
                            String key = resultSet.getString(keyColumn);
                            String value = resultSet.getString(valueColumn);
                            return KV.of(key, value);
                        }))
                .apply(viewName, View.asMap());
    }
}
