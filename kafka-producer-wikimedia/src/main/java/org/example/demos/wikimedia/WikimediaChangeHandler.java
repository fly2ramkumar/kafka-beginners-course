package org.example.demos.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {

    private final KafkaProducer<String, String> kafkaProducer;
    private final String topic;
    private final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

    public WikimediaChangeHandler( String topic, KafkaProducer<String, String> kafkaProducer){
        this.topic = topic;
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public void onOpen()  {
        // nothing here
    }

    @Override
    public void onClosed()  {
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent)  {
        log.info("Sending data - {}",messageEvent.getData());
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String s)  {
        // nothing here
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("Error in stream reading ", throwable);
    }
}
