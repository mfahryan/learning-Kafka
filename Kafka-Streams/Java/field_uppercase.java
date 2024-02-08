import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import io.confluent.connect.jms.Value;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class AvroStreams {
    public static void main(String[] args) {

        String bootstrapServers = "localhost:9094";
        String schemaRegistryUrl = "https://localhost:8081";
        String inputTopic = "mq-source-1";
        String outputTopic = "mq-source-1-output";

        Properties props = new Properties();

        Map<String,String> serdeConfig = new HashMap<>();

        serdeConfig.put("schema.registry.url", schemaRegistryUrl);
        serdeConfig.put("schema.registry.ssl.truststore.location", "hidden");
        serdeConfig.put("schema.registry.ssl.truststore.password", "hidden");
        serdeConfig.put("basic.auth.user.info", "admin:password");
        serdeConfig.put("basic.auth.credentials.source", "USER_INFO");

        SpecificAvroSerde<Value> mq_sourceserde = new SpecificAvroSerde<>();
        mq_sourceserde.configure(serdeConfig, false);

        props.put("bootstrap.servers", "localhost:9094");
        props.put("ssl.truststore.location", "hidden");
        props.put("security.protocol", "SASL_SSL");
        props.put("ssl.truststore.password", "hidden");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"password\";");
        props.put("basic.auth.user.info", "admin:password");
        props.put("basic.auth.credentials.source", "USER_INFO");

        props.put("schema.registry.url", "https://localhost:8081");
        props.put("schema.registry.ssl.truststore.location", "hidden");
        props.put("schema.registry.ssl.truststore.password", "hidden");
        props.put("schema.registry.basic.auth.user.info", "admin:password");
        props.put("schema.registry.basic.auth.credentials.source", "USER_INFO");


        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "secure-kafka-streams-app");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class.getName());


        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG), bootstrapServers);
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.METADATA_MAX_AGE_CONFIG),60000);
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG), "false");
        props.put(StreamsConfig.consumerPrefix("key.deserializer"), "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(StreamsConfig.consumerPrefix("value.deserializer"), "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put(StreamsConfig.consumerPrefix("specific.avro.reader"), "true");
        props.put(StreamsConfig.consumerPrefix("sasl.mechanism"), "PLAIN");
        props.put(StreamsConfig.consumerPrefix("sasl.jaas.config"), "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"password\";");
        props.put(StreamsConfig.consumerPrefix("security.protocol"), "SASL_SSL");
        props.put(StreamsConfig.consumerPrefix("ssl.truststore.location"), "hidden");
        props.put(StreamsConfig.consumerPrefix("ssl.truststore.password"), "hidden");
        props.put(StreamsConfig.consumerPrefix("basic.auth.user.info"), "admin:password");
        props.put(StreamsConfig.consumerPrefix("basic.auth.credentials.source"), "USER_INFO");

        props.put(StreamsConfig.producerPrefix(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), bootstrapServers);

        props.put(StreamsConfig.producerPrefix(ProducerConfig.METADATA_MAX_AGE_CONFIG),60000);
        props.put(StreamsConfig.producerPrefix(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG),"org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(StreamsConfig.producerPrefix(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG), "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put(StreamsConfig.producerPrefix("sasl.mechanism"), "PLAIN");
        props.put(StreamsConfig.producerPrefix("sasl.jaas.config"), "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"password\";");
        props.put(StreamsConfig.producerPrefix("security.protocol"), "SASL_SSL");
        props.put(StreamsConfig.producerPrefix("ssl.truststore.location"), "hidden");
        props.put(StreamsConfig.producerPrefix("ssl.truststore.password"), "hidden");
        props.put(StreamsConfig.producerPrefix("basic.auth.user.info"), "admin:password");
        props.put(StreamsConfig.producerPrefix("basic.auth.credentials.source"), "USER_INFO");


        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(inputTopic, Consumed.with(Serdes.ByteArray(), mq_sourceserde))
                .mapValues(value -> {
                    // Ubah nilai email menjadi huruf besar
                    String uppercaseEmail = value.getText().toUpperCase();
                    return Value.newBuilder(value)
                            .setText(uppercaseEmail)
                            .build();
                })
                .to(outputTopic);

        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
    }
}
