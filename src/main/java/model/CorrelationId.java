package model;

import lombok.ToString;

import static java.lang.String.format;
import static java.util.UUID.randomUUID;

@ToString
public class CorrelationId {

    private final String id;

    CorrelationId(String title) {
        id = format("%s(%s)", title, randomUUID().toString());
    }

    public CorrelationId continueWith(String title) {
        return new CorrelationId(format("%s-%s", id, title));
    }
}
