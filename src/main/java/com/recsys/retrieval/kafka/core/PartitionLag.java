package com.demo.retrieval.kafka.core;

public record PartitionLag(int partitionId, long lag) {
    public int getPartitionId() { return partitionId; }
    public long getLag()        { return lag; }
}
