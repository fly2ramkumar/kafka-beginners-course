package org.example.demos.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoCooperative {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());

    public static void main(String[] args) {
       log.info("Hello World!");

        String groupId = "my-java-application";
        String topic = "demo_java";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());
        //properties.setProperty("group.instance.id", ".."); // For static assignments


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
       final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                log.info("Program about to be shutdown. Calling consumer wakeup method");
                consumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                   e.printStackTrace();
                }

            }
                                             }

        );

        try {

            consumer.subscribe(Arrays.asList(topic));

            while(true) {
               // log.info("Polling ");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for(ConsumerRecord<String, String> record : records){
                    log.info("key : {}, value : {}, partition: {}, offset: {}",
                            record.key(), record.value(), record.partition(), record.offset());
                }

            }

        } catch(WakeupException e){
            log.info("wakeup method initiated in Consumer" );
        } catch(Exception e){
            log.error("Error while consuming messages", e );
        } finally {
            consumer.close();
        }




    }
}
