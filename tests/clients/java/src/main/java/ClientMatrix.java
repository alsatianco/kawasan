import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;

/** Each case owns its data; a failed prerequisite cannot hide another case. */
public class ClientMatrix {
    static final String BOOTSTRAP = System.getenv().getOrDefault("BOOTSTRAP", "localhost:9092");
    static final String RUN = "cm1-" + UUID.randomUUID();
    static final Duration WAIT = Duration.ofSeconds(30);
    static final ObjectMapper JSON = new ObjectMapper();
    interface Case { void run() throws Exception; }
    static Properties base() {
        Properties p = new Properties();
        p.put("bootstrap.servers", BOOTSTRAP);
        p.put("request.timeout.ms", "10000");
        p.put("default.api.timeout.ms", "30000");
        p.put("client.id", "cm1-java");
        return p;
    }
    static KafkaProducer<String, String> producer(String txn) {
        Properties p = base();
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("enable.idempotence", "true");
        p.put("acks", "all");
        p.put("max.block.ms", "30000");
        p.put("delivery.timeout.ms", "30000");
        p.put("transaction.timeout.ms", "30000");
        if (txn != null) p.put("transactional.id", txn);
        return new KafkaProducer<>(p);
    }
    static KafkaConsumer<String, String> consumer(String group) {
        Properties p = base();
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("group.id", group);
        p.put("group.protocol", "classic");
        p.put("enable.auto.commit", "false");
        p.put("auto.offset.reset", "earliest");
        p.put("isolation.level", "read_committed");
        return new KafkaConsumer<>(p);
    }
    static <T> T await(org.apache.kafka.common.KafkaFuture<T> f) throws Exception {
        return f.get(30, TimeUnit.SECONDS);
    }
    static void check(boolean b, String msg) { if (!b) throw new AssertionError(msg); }
    static String topic(String suffix) throws Exception {
        String t = RUN + "-" + suffix;
        try (Admin a = Admin.create(base())) {
            await(a.createTopics(List.of(new NewTopic(t, 1, (short)1))).all());
        }
        return t;
    }
    static List<ConsumerRecord<String, String>> read(KafkaConsumer<String, String> c, int n) {
        List<ConsumerRecord<String, String>> out = new ArrayList<>();
        long end = System.nanoTime() + WAIT.toNanos();
        while (out.size() < n && System.nanoTime() < end) c.poll(Duration.ofMillis(250)).forEach(out::add);
        check(out.size() == n, "expected " + n + " records, got " + out.size());
        return out;
    }
    static List<String> scan(String t, int expected) {
        try (KafkaConsumer<String, String> c = consumer(RUN + "-scan-" + t)) {
            TopicPartition tp = new TopicPartition(t, 0);
            c.assign(List.of(tp)); c.seekToBeginning(List.of(tp));
            List<String> values = new ArrayList<>();
            long end = System.nanoTime() + WAIT.toNanos();
            long quiet = 0;
            while (System.nanoTime() < end) {
                var records = c.poll(Duration.ofMillis(250));
                records.forEach(r -> values.add(r.value()));
                if (values.size() >= expected && records.isEmpty()) {
                    if (++quiet >= 8) break;
                } else quiet = 0;
            }
            return values;
        }
    }
    static void adminTopics() throws Exception {
        String t = topic("admin-topics");
        try (Admin a = Admin.create(base())) {
            check(await(a.listTopics().names()).contains(t), "created topic missing");
            check(await(a.describeTopics(List.of(t)).allTopicNames()).get(t).partitions().size() == 1, "partition count");
            await(a.deleteTopics(List.of(t)).all());
            long end = System.nanoTime() + WAIT.toNanos();
            while (await(a.listTopics().names()).contains(t) && System.nanoTime() < end) Thread.sleep(100);
            check(!await(a.listTopics().names()).contains(t), "deleted topic remains");
        }
    }
    static void adminConfigs() throws Exception {
        String t = topic("configs");
        ConfigResource r = new ConfigResource(ConfigResource.Type.TOPIC, t);
        try (Admin a = Admin.create(base())) {
            await(a.incrementalAlterConfigs(Map.of(r, List.of(new AlterConfigOp(new ConfigEntry("retention.ms", "300000"), AlterConfigOp.OpType.SET)))).all());
            check("300000".equals(await(a.describeConfigs(List.of(r)).all()).get(r).get("retention.ms").value()), "config round trip");
        }
    }
    static void adminCluster() throws Exception {
        try (Admin a = Admin.create(base())) {
            var d = a.describeCluster();
            var nodes = await(d.nodes()); var controller = await(d.controller());
            check(!nodes.isEmpty() && controller != null && nodes.stream().anyMatch(n -> n.id() == controller.id()), "controller not a broker");
        }
    }
    static void adminGroups() throws Exception {
        String t = topic("groups"); String group = RUN + "-groups";
        try (KafkaProducer<String,String> p = producer(null)) { p.send(new ProducerRecord<>(t, 0, "k", "group-record")).get(30, TimeUnit.SECONDS); }
        try (KafkaConsumer<String,String> c = consumer(group); Admin a = Admin.create(base())) {
            c.subscribe(List.of(t)); read(c, 1); c.commitSync();
            check(await(a.listConsumerGroups().all()).stream().anyMatch(g -> g.groupId().equals(group)), "active group missing");
            check(!await(a.describeConsumerGroups(List.of(group)).all()).get(group).members().isEmpty(), "group members missing");
        }
    }
    static void adminLogDirs() throws Exception {
        String t = topic("logdirs");
        try (KafkaProducer<String,String> p = producer(null)) { p.send(new ProducerRecord<>(t, 0, "k", "logdir-record")).get(30, TimeUnit.SECONDS); }
        try (Admin a = Admin.create(base())) {
            var ids = await(a.describeCluster().nodes()).stream().map(Node::id).toList();
            var dirs = await(a.describeLogDirs(ids).allDescriptions());
            check(dirs.values().stream().flatMap(d -> d.values().stream()).anyMatch(d -> d.replicaInfos().containsKey(new TopicPartition(t, 0)) && d.replicaInfos().get(new TopicPartition(t, 0)).size() > 0), "log dir lacks nonempty replica");
        }
    }
    static void adminAcls() throws Exception {
        try (Admin a = Admin.create(base())) { await(a.describeAcls(AclBindingFilter.ANY).values()); }
    }
    static void produceConsume() throws Exception {
        String t = topic("roundtrip");
        try (KafkaProducer<String,String> p = producer(null)) {
            for (int i=0;i<10;i++) check(p.send(new ProducerRecord<>(t, 0, "k", (System.getenv("CM1_SEED_PRODUCE_BUG") != null && i == 0 ? "corrupt-value" : "value-"+i))).get(30, TimeUnit.SECONDS).offset() == i, "produce offset");
        }
        var expected = java.util.stream.IntStream.range(0,10).mapToObj(i -> "value-"+i).toList();
        check(scan(t, 10).equals(expected), "record values/order");
    }
    static void transactions() throws Exception {
        String t = topic("transactions");
        try (KafkaProducer<String,String> p = producer(RUN+"-txn")) {
            p.initTransactions();
            p.beginTransaction(); p.send(new ProducerRecord<>(t, 0, "k", "committed")).get(30, TimeUnit.SECONDS); p.commitTransaction();
            p.beginTransaction(); p.send(new ProducerRecord<>(t, 0, "k", "aborted")).get(30, TimeUnit.SECONDS); p.abortTransaction();
        }
        check(scan(t, 1).equals(List.of("committed")), "abort visible or commit missing");
    }
    static void eosOffsets() throws Exception {
        String input = topic("eos-in"), output = topic("eos-out"), group = RUN+"-eos-group";
        try (KafkaProducer<String,String> seed = producer(null)) {
            seed.send(new ProducerRecord<>(input, 0, "k", "first")).get(30, TimeUnit.SECONDS);
            seed.send(new ProducerRecord<>(input, 0, "k", "second")).get(30, TimeUnit.SECONDS);
        }
        TopicPartition tp = new TopicPartition(input, 0);
        try (KafkaConsumer<String,String> c = consumer(group); KafkaProducer<String,String> p = producer(RUN+"-eos")) {
            c.subscribe(List.of(input)); read(c, 2); p.initTransactions();
            p.beginTransaction(); p.send(new ProducerRecord<>(output, 0, "k", "committed")).get(30, TimeUnit.SECONDS);
            p.sendOffsetsToTransaction(Map.of(tp, new OffsetAndMetadata(1)), c.groupMetadata()); p.commitTransaction();
            check(c.committed(Set.of(tp)).get(tp).offset() == 1, "committed offset missing");
            p.beginTransaction(); p.send(new ProducerRecord<>(output, 0, "k", "aborted")).get(30, TimeUnit.SECONDS);
            p.sendOffsetsToTransaction(Map.of(tp, new OffsetAndMetadata(2)), c.groupMetadata()); p.abortTransaction();
            check(c.committed(Set.of(tp)).get(tp).offset() == 1, "aborted offset applied");
        }
        check(scan(output, 1).equals(List.of("committed")), "EOS visibility");
    }
    static void fencing() throws Exception {
        String t = topic("fencing"), id = RUN+"-fenced";
        try (KafkaProducer<String,String> old = producer(id); KafkaProducer<String,String> fresh = producer(id)) {
            old.initTransactions(); old.beginTransaction();
            old.send(new ProducerRecord<>(t, 0, "k", "old")).get(30, TimeUnit.SECONDS);
            fresh.initTransactions();
            boolean fenced = false;
            try { old.commitTransaction(); } catch (org.apache.kafka.common.errors.ProducerFencedException | org.apache.kafka.common.errors.InvalidProducerEpochException e) { fenced = true; }
            check(fenced, "obsolete producer was not fenced");
            fresh.beginTransaction(); fresh.send(new ProducerRecord<>(t, 0, "k", "fresh")).get(30, TimeUnit.SECONDS); fresh.commitTransaction();
        }
        check(scan(t, 1).equals(List.of("fresh")), "fenced transaction visible");
    }
    // A wire probe prevents ordinary successful round trips from hiding downgrades.
    static void apiParity() throws Exception {
        String[] endpoint = BOOTSTRAP.split(":");
        Map<Integer,Integer> versions = new HashMap<>();
        try (Socket s = new Socket(endpoint[0], Integer.parseInt(endpoint[1]))) {
            s.setSoTimeout(10000);
            DataOutputStream out = new DataOutputStream(s.getOutputStream());
            out.writeInt(10); out.writeShort(18); out.writeShort(0); out.writeInt(42); out.writeShort(0); out.flush();
            DataInputStream in = new DataInputStream(s.getInputStream());
            int size = in.readInt(); check(size > 10 && size < 100000, "ApiVersions frame size");
            byte[] bytes = in.readNBytes(size); check(bytes.length == size, "truncated ApiVersions");
            ByteBuffer b = ByteBuffer.wrap(bytes); check(b.getInt() == 42 && b.getShort() == 0, "ApiVersions error");
            int count = b.getInt();
            for (int i=0;i<count;i++) { int key = b.getShort(); b.getShort(); versions.put(key, (int)b.getShort()); }
        }
        Map<Integer,Integer> required = Map.of(0,11,1,13,2,8,9,9,21,2,36,2);
        check(required.entrySet().stream().allMatch(e -> versions.getOrDefault(e.getKey(),-1) >= e.getValue()), "api parity: " + versions);
    }
    public static void main(String[] args) throws Exception {
        LinkedHashMap<String, Case> cases = new LinkedHashMap<>();
        cases.put("admin-topics", ClientMatrix::adminTopics); cases.put("admin-configs", ClientMatrix::adminConfigs);
        cases.put("admin-cluster", ClientMatrix::adminCluster); cases.put("admin-groups", ClientMatrix::adminGroups);
        cases.put("admin-log-dirs", ClientMatrix::adminLogDirs); cases.put("admin-acls", ClientMatrix::adminAcls);
        cases.put("produce-consume", ClientMatrix::produceConsume); cases.put("transactions", ClientMatrix::transactions);
        cases.put("eos-offsets", ClientMatrix::eosOffsets); cases.put("producer-fencing", ClientMatrix::fencing);
        cases.put("api-parity", ClientMatrix::apiParity);
        try (PrintWriter results = new PrintWriter(new FileWriter(System.getenv().getOrDefault("RESULTS", "/evidence/results.jsonl")))) {
            for (var entry : cases.entrySet()) {
                try {
                    entry.getValue().run();
                    results.println(JSON.writeValueAsString(Map.of("case",entry.getKey(),"status","PASS")));
                } catch (Throwable e) {
                    e.printStackTrace();
                    while (e.getCause() != null) e = e.getCause();
                    results.println(JSON.writeValueAsString(Map.of("case",entry.getKey(),"status","FAIL","error",e.getClass().getSimpleName()+": "+e.getMessage())));
                }
                results.flush();
            }
        }
    }
}
