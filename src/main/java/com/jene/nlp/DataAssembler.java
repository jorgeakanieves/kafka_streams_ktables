package com.jene.nlp;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Data assembler from messages
 *
 * @author Jorge Nieves (jene)
 */
public class DataAssembler {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataAssembler.class);

    public static void main(String[] args) throws Exception {

        Properties props;
        if (args.length==1)
            props = Configs.loadConfig(args[0]);
        else
            props = Configs.loadConfig();

        AdminClient ac = AdminClient.create(props);
        DescribeClusterResult dcr = ac.describeCluster();
        int clusterSize = dcr.nodes().get().size();

        if (clusterSize<3)
            props.put("replication.factor",clusterSize);
        else
            props.put("replication.factor",3);

        props.forEach((key, value) -> LOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ::: Property key: " + key+ " -> value: " + value));

        final Serde<String> stringSerde = Serdes.String();

        final StreamsBuilder builder = new StreamsBuilder();

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss:SS");

        String[] topics = props.getProperty("input-topics").split(",");

        List<KTable<String, String>> tables = new ArrayList<>();
        int max = topics.length - 1;

        for (String topic: topics) {
            tables.add(builder.table(topic, Consumed.with(stringSerde, stringSerde)));
        }

        int i = 0;
        List<KTable<String, String>> joins = new ArrayList<>();
        while(i<max){
            if(i>0) {
                joins.add(i, joins.get(i-1).join(tables.get(i + 1),
                        (l, r) -> l + ',' + r
                ));
            } else {
                joins.add(i, tables.get(i).join(tables.get(i + 1),
                        (l, r) -> l + ',' + r
                ));
            }
            i++;
        }

        joins.get(i-1).toStream().to(props.getProperty("output-topic"));

        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, props);

        LOGGER.info(topology.describe().toString());

        streams.cleanUp();

        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


    }
}
