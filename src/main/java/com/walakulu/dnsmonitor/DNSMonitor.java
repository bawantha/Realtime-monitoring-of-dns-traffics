package com.walakulu.dnsmonitor;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class DNSMonitor {

    public static final String INPUT_TOPIC = "DNSLogInputTopic";
    public static final String OUTPUT_TOPIC = "DNSLogOutputTopic";


    public static void execute(String bootstrapServers, String applictionId, String modelClassName) throws Exception {

        final Properties streamsConfiguration = getStreamConfiguration(bootstrapServers, applictionId);
        Topology topology = getStreamTopology(modelClassName);

        // Start Kafka Streams Application to process new incoming messages from Input
        // Topic
        final KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);
        streams.cleanUp();
        streams.start();
        System.out.println("DNS Anomaly Detection Microservice is running...");
        System.out.println("Input to Kafka Topic 'DNSLogInputTopic'; Output to Kafka Topic 'DNSLogOutputTopic'");

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka
        // Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    static Properties getStreamConfiguration(String bootstrapServers, String applicationId) {
        final Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name. The name must be unique
        // in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Specify default (de)serializers for record keys and for record
        // values.
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // For illustrative purposes we disable record caches
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        return streamsConfiguration;
    }

    static Topology getStreamTopology(String modelClassName) throws InstantiationException, IllegalAccessException, ClassNotFoundException, ArrayIndexOutOfBoundsException {
        // Create H2O object (see gbm_pojo_test.java)
//        hex.genmodel.GenModel rawModel;
//        rawModel = (hex.genmodel.GenModel) Class.forName(modelClassName).newInstance();
//        EasyPredictModelWrapper model = new EasyPredictModelWrapper(rawModel);

        // In the subsequent lines we define the processing topology of the
        // Streams application.
        final StreamsBuilder builder = new StreamsBuilder();

        // Construct a `KStream` from the input topic "AirlineInputTopic", where
        // message values
        // represent lines of text (for the sake of this example, we ignore
        // whatever may be stored
        // in the message keys).
        final KStream<String, String> dnsLogInput = builder.stream(INPUT_TOPIC);

        // Stream Processor (in this case 'mapValues' to add custom logic, i.e. apply
        // the analytic model)
        KStream<String, String> transformedMessage =
                dnsLogInput.mapValues(value->{
                    String  values[] =value.split(" ");
                    String csvValue="";
                    csvValue+=values[0] ;//csvValue.concat(values[0]);// Date
                    csvValue+="," ;
                    csvValue+=values[1];   // Time;
                    csvValue+="," ;
                    String client_ip=values[6].split("#")[0];
                    String port=values[6].split("#")[1];
                    csvValue+=client_ip;  // Client IP;
                    csvValue+=",";
                    csvValue+=port; // Port
                    csvValue+=",";
                    csvValue+=values[9]; // request URL
                    csvValue+=",";
                    csvValue+=values[11]; // record Type
                    csvValue+=",";
                    csvValue+=values[12]; // flag
                    csvValue+=",";
                    csvValue+=values[13].substring(1,values[13].length()-1); // nameserver IP
                    return  csvValue;
                });

        // Send prediction information to Output Topic
        transformedMessage.to(OUTPUT_TOPIC);
        return builder.build();
    }
}
