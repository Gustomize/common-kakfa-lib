package consumer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.stream.IntStream.range;

public class ServiceRunner<T> {
    private final ServiceProvider<T> provider;

    public ServiceRunner(ServiceFactory<T> factory) {
        this.provider = new ServiceProvider<>(factory);
    }

    public void start(int threadCount) {
        ExecutorService poll = newFixedThreadPool(threadCount);
        range(0, threadCount).mapToObj(i -> provider).forEach(poll::submit);
    }
}
