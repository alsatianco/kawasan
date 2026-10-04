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
        var defaultConfig = new DescribeConfigsResponseData.DescribeConfigsResourceResult().setName("retention.ms").setValue("604800000").setConfigSource((byte)5).setDocumentation(null);
        var topicConfig = new DescribeConfigsResponseData.DescribeConfigsResourceResult().setName("segment.bytes").setValue("4096").setConfigSource((byte)1).setDocumentation(null);
        var configResult = new DescribeConfigsResponseData.DescribeConfigsResult().setErrorMessage(null).setResourceType((byte)2).setResourceName("t").setConfigs(List.of(defaultConfig, topicConfig));
        for (short v : new short[]{1,4}) add("describe-configs-response-v"+v,new DescribeConfigsResponseData().setResults(List.of(configResult)),v);
        var logTopics = new DescribeLogDirsRequestData.DescribableLogDirTopicCollection();
        logTopics.add(new DescribeLogDirsRequestData.DescribableLogDirTopic().setTopic("t").setPartitions(List.of(0)));
        add("describe-log-dirs-request-v1",new DescribeLogDirsRequestData().setTopics(logTopics),(short)1);
        add("describe-log-dirs-all-request-v1",new DescribeLogDirsRequestData().setTopics(null),(short)1);
        var logPartition = new DescribeLogDirsResponseData.DescribeLogDirsPartition().setPartitionIndex(0).setPartitionSize(4096).setOffsetLag(3).setIsFutureKey(false);
        var logTopic = new DescribeLogDirsResponseData.DescribeLogDirsTopic().setName("t").setPartitions(List.of(logPartition));
        var logResult = new DescribeLogDirsResponseData.DescribeLogDirsResult().setLogDir("/logs").setTopics(List.of(logTopic));
        add("describe-log-dirs-response-v1",new DescribeLogDirsResponseData().setResults(List.of(logResult)),(short)1);
        add("describe-acls-request-v1",new DescribeAclsRequestData().setResourceTypeFilter((byte)2).setResourceNameFilter("t")
            .setPatternTypeFilter((byte)3).setPrincipalFilter("User:a").setHostFilter("*").setOperation((byte)3).setPermissionType((byte)3),(short)1);
        var acl = new DescribeAclsResponseData.AclDescription().setPrincipal("User:a").setHost("*").setOperation((byte)3).setPermissionType((byte)3);
        var aclResource = new DescribeAclsResponseData.DescribeAclsResource().setResourceType((byte)2).setResourceName("t").setPatternType((byte)3).setAcls(List.of(acl));
        add("describe-acls-response-v1",new DescribeAclsResponseData().setErrorMessage(null).setResources(List.of(aclResource)),(short)1);
        var txnTopics = new AddPartitionsToTxnRequestData.AddPartitionsToTxnTopicCollection();
        txnTopics.add(new AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic().setName("t").setPartitions(List.of(0)));
        add("add-partitions-to-txn-request-v3",new AddPartitionsToTxnRequestData().setV3AndBelowTransactionalId("tx")
            .setV3AndBelowProducerId(42).setV3AndBelowProducerEpoch((short)7).setV3AndBelowTopics(txnTopics),(short)3);
        var txnPartitionResults = new AddPartitionsToTxnResponseData.AddPartitionsToTxnPartitionResultCollection();
        txnPartitionResults.add(new AddPartitionsToTxnResponseData.AddPartitionsToTxnPartitionResult().setPartitionIndex(0).setPartitionErrorCode((short)47));
        var txnTopicResults = new AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResultCollection();
        txnTopicResults.add(new AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResult().setName("t").setResultsByPartition(txnPartitionResults));
        add("add-partitions-to-txn-response-v3",new AddPartitionsToTxnResponseData().setResultsByTopicV3AndBelow(txnTopicResults),(short)3);
        add("add-offsets-to-txn-request-v3",new AddOffsetsToTxnRequestData().setTransactionalId("tx").setProducerId(42)
            .setProducerEpoch((short)7).setGroupId("g"),(short)3);
        add("add-offsets-to-txn-response-v3",new AddOffsetsToTxnResponseData().setErrorCode((short)47),(short)3);
        add("end-txn-request-v3",new EndTxnRequestData().setTransactionalId("tx").setProducerId(42).setProducerEpoch((short)7).setCommitted(true),(short)3);
        add("end-txn-response-v3",new EndTxnResponseData().setErrorCode((short)47),(short)3);
        var commitPartition = new TxnOffsetCommitRequestData.TxnOffsetCommitRequestPartition().setPartitionIndex(0).setCommittedOffset(42)
            .setCommittedLeaderEpoch(3).setCommittedMetadata("meta");
        var commitTopic = new TxnOffsetCommitRequestData.TxnOffsetCommitRequestTopic().setName("t").setPartitions(List.of(commitPartition));
        var commitRequest = new TxnOffsetCommitRequestData().setTransactionalId("tx").setGroupId("g").setProducerId(42)
            .setProducerEpoch((short)7).setTopics(List.of(commitTopic));
        add("txn-offset-commit-request-v2",commitRequest,(short)2);
        commitRequest.setGenerationId(9).setMemberId("m").setGroupInstanceId("instance");
        add("txn-offset-commit-request-v3",commitRequest,(short)3);
        commitRequest.setGroupInstanceId(null);
        add("txn-offset-commit-dynamic-request-v3",commitRequest,(short)3);
        var commitResultPartition = new TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition().setPartitionIndex(0).setErrorCode((short)22);
        var commitResultTopic = new TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic().setName("t").setPartitions(List.of(commitResultPartition));
        add("txn-offset-commit-response-v3",new TxnOffsetCommitResponseData().setTopics(List.of(commitResultTopic)),(short)3);
        System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(fixtures));
    }
}
