package io.github.hectorvent.floci.services.msk.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

/** A topic managed through the MSK topic API; {@code configs} is the base64 JSON the API carries. */
@RegisterForReflection
public class MskTopic {

    private String topicArn;
    private String clusterArn;
    private String topicName;
    private int partitionCount;
    private int replicationFactor;
    private String configs;

    public String getTopicArn() { return topicArn; }

    public void setTopicArn(String topicArn) { this.topicArn = topicArn; }

    public String getClusterArn() { return clusterArn; }

    public void setClusterArn(String clusterArn) { this.clusterArn = clusterArn; }

    public String getTopicName() { return topicName; }

    public void setTopicName(String topicName) { this.topicName = topicName; }

    public int getPartitionCount() { return partitionCount; }

    public void setPartitionCount(int partitionCount) { this.partitionCount = partitionCount; }

    public int getReplicationFactor() { return replicationFactor; }

    public void setReplicationFactor(int replicationFactor) { this.replicationFactor = replicationFactor; }

    public String getConfigs() { return configs; }

    public void setConfigs(String configs) { this.configs = configs; }
}
