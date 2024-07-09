package com.sirclo.beam.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface StreamInsertOptions extends PipelineOptions {
    @Description("Project ID")
    String getProjectId();
    void setProjectId(String projectId);

    @Description("Subscription Name of PubSub")
    String getSubscriptionName();
    void setSubscriptionName(String subscriptionName);

    @Description("Target table name")
    String getTableName();
    void setTableName(String tableName);

    @Description("Target database username")
    String getUsername();
    void setUsername(String username);

    @Description("Target database password")
    String getPassword();
    void setPassword(String password);

    @Description("Target database URL")
    String getJdbcUrl();
    void setJdbcUrl(String jdbcUrl);

}
