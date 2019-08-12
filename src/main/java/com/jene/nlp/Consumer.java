package com.jene.nlp;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Properties;


/**
 * Consume messages from output topic to check assembled message is correct with content and time delivered
 *
 * @author Jorge Nieves (jene)
 */
public class Consumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataAssembler.class);

    public static String topic = "output-assembler";

    public static void main(String[] args) throws Exception {

        Properties props;
        if (args.length==1)
            props = Configs.loadConfig(args[0]);
        else
            props = Configs.loadConfig();

/*        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");*/

        props.forEach((key, value) -> LOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ::: Property key: " + key+ " -> value: " + value));

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topic));

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss:SS");

        // Loop until ctrl + c
        int count = 0;
        while(true) {
            // Poll for records
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Integer.parseInt((props.getProperty("poll-ms").toString()))));
            // Did we get any?
            if (records.count() == 0) {
                // timeout/nothing to read
            } else {
                // Yes, loop over records
                for(ConsumerRecord<String, String> record: records) {
                    // Display record and count
                    count += 1;
                    //System.out.println(">>>>>> "+ count + ": " + record.value() + " and time: " + dtf.format(LocalDateTime.now()));
                    LOGGER.info("{"+ dtf.format(LocalDateTime.now()) + "]" + " >>>>>> "+ record.value());
                }
            }
        }

    }
}



