package com.jene.nlp;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Produce messages to output topics to emulate async services checking time delivered with assembler service
 *
 * @author Jorge Nieves (jene)
 */
public class Producer {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataAssembler.class);
    public static KafkaProducer<String, String> producer = null;


    public static void main(String[] args) throws Exception {
        Properties props;
        if (args.length==1)
            props = Configs.loadConfig(args[0]);
        else
            props = Configs.loadConfig();

        props.forEach((key, value) -> LOGGER.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ::: Property key: " + key+ " -> value: " + value));

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss:SS");

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Shutting Down");
                if (producer != null)
                    producer.close();
            }
        });


        // Starting producer
        producer = new KafkaProducer<>(props);

        Map<String, Map<String, String>> msgs = new LinkedHashMap<>();
        // nº chat messages
        int count = Integer.parseInt(props.getProperty("num-produced-msgs"));
        int i = 1;
        int countMsg = 1;
        String[] topics = props.getProperty("input-topics").split(",");
        // nº topics
        int countTopics = topics.length;

        while(i<=count){
            UUID uuid = UUID.randomUUID();

            // nº of topics
            int j = 0;
            while(j<countTopics){
                Map <String, String> msg = new HashMap<>();
                //msg.put(uuid.toString(), "m:" + countMsg + ",tp:" + j + ",t:" + dtf.format(LocalDateTime.now()));
                msg.put(uuid.toString(), "m:" + countMsg);
                msgs.put(topics[j]+"_"+i, msg);
                j++;
                countMsg++;
            }
            i++;
        }

        Iterator it = msgs.entrySet().iterator();
        while (it.hasNext()) {
            /*Map.Entry pair = (Map.Entry) it.next();
            System.out.println(pair.getKey() + " = " + pair.getValue());*/
            Map.Entry pair = (Map.Entry) it.next();
            HashMap<String, String> pair2 = (HashMap<String, String>) pair.getValue();
            Map.Entry<String,String> entry = pair2.entrySet().iterator().next();
            String key = entry.getKey();
            String value = entry.getValue();
            String topic = pair.getKey().toString().substring(0, pair.getKey().toString().indexOf("_"));
            String message = "{\"topic\":\""+topic+"\", message:\""+value+"\"}";

            ProducerRecord<String, String> record = new ProducerRecord(topic, key, message);

            producer.send(record, (RecordMetadata r, Exception e) -> {
                if (e != null) {
                    LOGGER.error("Error producing events");
                    e.printStackTrace();
                }
            });

            LOGGER.info("{"+ dtf.format(LocalDateTime.now()) + "]" + " >>>>> Produce msg on topic " + topic + ": key:"+ key + " , value: " + pair.getValue());

            //Thread.sleep(1000);
        }

    }
    private static final String ALPHA_NUMERIC_STRING = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    public static String randomAlphaNumeric(int count) {
        StringBuilder builder = new StringBuilder();
        while (count-- != 0) {
            int character = (int)(Math.random()*ALPHA_NUMERIC_STRING.length());
            builder.append(ALPHA_NUMERIC_STRING.charAt(character));
        }
        return builder.toString();
    }

}
