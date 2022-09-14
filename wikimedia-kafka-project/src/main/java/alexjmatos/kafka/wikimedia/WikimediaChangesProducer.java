package alexjmatos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";

        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the producer
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)){
            String topic = "wikimedia.recentchange";
            EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
            String url = "https://stream.wikimedia.org/v2/stream/recentchange";
            EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
            EventSource eventSource = builder.build();
            eventSource.start();
            TimeUnit.MINUTES.sleep(10);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
