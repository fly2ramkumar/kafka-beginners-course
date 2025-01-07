package org.example.demos.producers;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {
       log.info("Hello World!");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j=0; j <2; j++){

            for (int i = 0; i < 10; i++) {

                String key = "id_"+i;
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", key, "Hello World! "+i);

                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                        if(e == null){
                            log.info("Received new metadata Key - {}, Topic - {}, Partition - {}",
                                    key, recordMetadata.topic(), recordMetadata.partition()
                            );
                        } else {
                            log.error("Error while producing message - ", e);

                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }



        producer.flush();

        producer.close();

    }
}
