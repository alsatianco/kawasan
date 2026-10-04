import java.nio.ByteBuffer;
import java.util.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.protocol.*;

/** Golden bytes emitted by Kafka's generated codecs, independently of Kawasan. */
public class WireFixtures {
    static Map<String,String> fixtures = new TreeMap<>();
    static void add(String name, Message m, short v) {
        ObjectSerializationCache cache = new ObjectSerializationCache();
        ByteBuffer b = ByteBuffer.allocate(m.size(cache,v));
        m.write(new ByteBufferAccessor(b),cache,v);
        fixtures.put(name, HexFormat.of().formatHex(b.array()));
    }
    public static void main(String[] args) throws Exception {
        var member = new DescribeGroupsResponseData.DescribedGroupMember()
            .setMemberId("m").setGroupInstanceId("instance").setClientId("client").setClientHost("host")
            .setMemberMetadata(new byte[]{1,2}).setMemberAssignment(new byte[]{3,4});
        var group = new DescribeGroupsResponseData.DescribedGroup().setGroupId("g")
            .setGroupState("Stable").setProtocolType("consumer").setProtocolData("range")
            .setMembers(List.of(member)).setAuthorizedOperations(8);
        var response = new DescribeGroupsResponseData().setGroups(List.of(group));
        add("describe-groups-v4",response,(short)4);
        add("describe-groups-v5",response,(short)5);
        member.setGroupInstanceId(null);
        add("describe-groups-null-v4",response,(short)4);
        add("describe-groups-null-v5",response,(short)5);

        add("sasl-request-v2", new SaslAuthenticateRequestData().setAuthBytes(new byte[]{1,2,3}), (short)2);
        add("sasl-response-v2", new SaslAuthenticateResponseData().setErrorCode((short)58)
            .setErrorMessage("denied").setAuthBytes(new byte[]{4,5}).setSessionLifetimeMs(1234), (short)2);
        var deletePartition = new DeleteRecordsRequestData.DeleteRecordsPartition().setPartitionIndex(0).setOffset(42);
        var deleteTopic = new DeleteRecordsRequestData.DeleteRecordsTopic().setName("t").setPartitions(List.of(deletePartition));
        add("delete-records-request-v2",new DeleteRecordsRequestData().setTopics(List.of(deleteTopic)).setTimeoutMs(5000),(short)2);
        var deleteResult = new DeleteRecordsResponseData.DeleteRecordsPartitionResult().setPartitionIndex(0).setLowWatermark(42);
        var deletePartitions = new DeleteRecordsResponseData.DeleteRecordsPartitionResultCollection();
        deletePartitions.add(deleteResult);
        var deleteResults = new DeleteRecordsResponseData.DeleteRecordsTopicResultCollection();
        deleteResults.add(new DeleteRecordsResponseData.DeleteRecordsTopicResult().setName("t").setPartitions(deletePartitions));
        add("delete-records-response-v2",new DeleteRecordsResponseData().setTopics(deleteResults),(short)2);
        var offsetTopic = new OffsetFetchRequestData.OffsetFetchRequestTopics().setName("t").setPartitionIndexes(List.of(0));
        var offsetGroup = new OffsetFetchRequestData.OffsetFetchRequestGroup().setGroupId("g").setMemberId("m").setMemberEpoch(7).setTopics(List.of(offsetTopic));
        add("offset-fetch-request-v9",new OffsetFetchRequestData().setGroups(List.of(offsetGroup)).setRequireStable(true),(short)9);
        offsetGroup.setMemberId(null).setMemberEpoch(-1);
        add("offset-fetch-classic-request-v9",new OffsetFetchRequestData().setGroups(List.of(offsetGroup)).setRequireStable(true),(short)9);
        var offsetResult = new OffsetFetchResponseData.OffsetFetchResponsePartitions().setPartitionIndex(0).setCommittedOffset(42).setCommittedLeaderEpoch(3).setMetadata("meta");
        var offsetResultTopic = new OffsetFetchResponseData.OffsetFetchResponseTopics().setName("t").setPartitions(List.of(offsetResult));
        var offsetResultGroup = new OffsetFetchResponseData.OffsetFetchResponseGroup().setGroupId("g").setTopics(List.of(offsetResultTopic));
        add("offset-fetch-response-v9",new OffsetFetchResponseData().setGroups(List.of(offsetResultGroup)),(short)9);
        Uuid uuid = new Uuid(0x0102030405060708L,0x090a0b0c0d0e0f10L);
        var fetchPartition = new FetchRequestData.FetchPartition().setPartition(0).setCurrentLeaderEpoch(-1).setFetchOffset(42)
            .setLastFetchedEpoch(-1).setLogStartOffset(-1).setPartitionMaxBytes(4096);
        var fetchTopic = new FetchRequestData.FetchTopic().setTopicId(uuid).setPartitions(List.of(fetchPartition));
        var forgotten = new FetchRequestData.ForgottenTopic().setTopicId(uuid).setPartitions(List.of(0));
        add("fetch-request-v13",new FetchRequestData().setReplicaId(-1).setMaxWaitMs(100).setMinBytes(1).setMaxBytes(4096)
            .setIsolationLevel((byte)1).setSessionId(0).setSessionEpoch(-1).setTopics(List.of(fetchTopic))
            .setForgottenTopicsData(List.of(forgotten)).setRackId("rack"),(short)13);
        var fetchResponses = new ArrayList<FetchResponseData.FetchableTopicResponse>();
        fetchResponses.add(new FetchResponseData.FetchableTopicResponse().setTopicId(uuid).setPartitions(List.of()));
        add("fetch-response-v13", new FetchResponseData().setResponses(fetchResponses), (short)13);
        var deleteTopics = new ArrayList<DeleteTopicsRequestData.DeleteTopicState>();
        deleteTopics.add(new DeleteTopicsRequestData.DeleteTopicState().setName("t").setTopicId(uuid));
        add("delete-topics-request-v6",new DeleteTopicsRequestData().setTopics(deleteTopics).setTimeoutMs(5000),(short)6);
        var deleteResponses = new DeleteTopicsResponseData.DeletableTopicResultCollection();
        deleteResponses.add(new DeleteTopicsResponseData.DeletableTopicResult().setName("t").setTopicId(uuid).setErrorMessage("error"));
        add("delete-topics-response-v6",new DeleteTopicsResponseData().setResponses(deleteResponses),(short)6);
        var produceTopics = new ProduceRequestData.TopicProduceDataCollection();
        produceTopics.add(new ProduceRequestData.TopicProduceData().setName("t").setPartitionData(List.of(
            new ProduceRequestData.PartitionProduceData().setIndex(0).setRecords(MemoryRecords.readableRecords(ByteBuffer.wrap(new byte[]{1,2}))))));
        var produceResults = new ProduceResponseData.TopicProduceResponseCollection();
        produceResults.add(new ProduceResponseData.TopicProduceResponse().setName("t").setPartitionResponses(List.of(
            new ProduceResponseData.PartitionProduceResponse().setIndex(0).setBaseOffset(42).setLogAppendTimeMs(-1).setLogStartOffset(0))));
        for (short v : new short[]{10,11}) {
            add("produce-request-v"+v,new ProduceRequestData().setAcks((short)-1).setTimeoutMs(5000).setTopicData(produceTopics),v);
            add("produce-response-v"+v,new ProduceResponseData().setResponses(produceResults),v);
        }
        var listPartition = new ListOffsetsRequestData.ListOffsetsPartition().setPartitionIndex(0).setCurrentLeaderEpoch(-1).setTimestamp(-4);
        var listTopic = new ListOffsetsRequestData.ListOffsetsTopic().setName("t").setPartitions(List.of(listPartition));
        add("list-offsets-request-v8",new ListOffsetsRequestData().setReplicaId(-1).setIsolationLevel((byte)1).setTopics(List.of(listTopic)),(short)8);
        var listResult = new ListOffsetsResponseData.ListOffsetsPartitionResponse().setPartitionIndex(0).setTimestamp(-1).setOffset(42).setLeaderEpoch(3);
        var listResultTopic = new ListOffsetsResponseData.ListOffsetsTopicResponse().setName("t").setPartitions(List.of(listResult));
        add("list-offsets-response-v8",new ListOffsetsResponseData().setTopics(List.of(listResultTopic)),(short)8);
        System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(fixtures));
    }
}
