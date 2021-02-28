package dispatcher;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import model.CorrelationId;
import model.Message;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.io.IOException;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.lang.String.format;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

@Slf4j
public class KafkaDispatcher<T> implements Closeable {
    private final KafkaProducer<String, Message<T>> producer;

    public KafkaDispatcher() {
        this.producer = new KafkaProducer<>(properties());
    }

    private Properties properties() {
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
        properties.setProperty(ACKS_CONFIG, "all");
        return properties;
    }

    public Future<RecordMetadata> sendAssync(String topic, String key, CorrelationId id, T payload) {
        Message<T> value = new Message<>(id.continueWith("_" + topic), payload);
        ProducerRecord<String, Message<T>> record = new ProducerRecord<>(topic, key, value);
        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            log.info(format("sucesso enviando %s:::partition %d/ offset %d/ timestamp %d%n",
                    data.topic(),
                    data.partition(),
                    data.offset(),
                    data.timestamp()));
        };
        return producer.send(record, callback);
    }

    @SneakyThrows({ExecutionException.class, InterruptedException.class})
    public void send(String topic, String key, CorrelationId id, T payload) {
        sendAssync(topic, key, id, payload).get();
    }

    @Override
    public void close() throws IOException {
        producer.close();
    }
}
