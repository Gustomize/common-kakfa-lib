package consumer;

import lombok.RequiredArgsConstructor;

import java.util.HashMap;
import java.util.concurrent.Callable;

@RequiredArgsConstructor
public class ServiceProvider<T> implements Callable<Void> {
    private final ServiceFactory<T> factory;

    @Override
    public Void call() throws Exception {
        ConsumerService<T> consumerService = factory.create();
        try (KafkaService service = new KafkaService(consumerService.getConsumerGroup(),
                consumerService.getTopic(),
                consumerService::parse,
                new HashMap<>())) {
            service.run();
        }
        return null;
    }
}
