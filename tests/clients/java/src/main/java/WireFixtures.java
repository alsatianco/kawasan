import java.nio.ByteBuffer;
import java.util.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.message.*;
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
        System.out.println(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(fixtures));
    }
}
