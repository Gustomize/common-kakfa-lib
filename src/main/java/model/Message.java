package model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@ToString
@RequiredArgsConstructor
public class Message<T> {
    @Getter
    private final CorrelationId id;

    @Getter
    private final T payload;
}
