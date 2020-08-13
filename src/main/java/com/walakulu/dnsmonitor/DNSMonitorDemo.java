package com.walakulu.dnsmonitor;

public class DNSMonitorDemo  extends  DNSMonitor{
    // Name of the generated H2O model
    static String modelClassName = "com.github.megachucky.kafka.streams.machinelearning.models.deeplearning_fe7c1f02_08ec_4070_b784_c2531147e451";

    static final String APPLICATION_ID = "dns-monitor";

    public static void main(final String[] args) throws Exception {

        // Configure Kafka Streams Application
        final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        execute(bootstrapServers, APPLICATION_ID, modelClassName);
    }
}
