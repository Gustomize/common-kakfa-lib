package consumer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import model.Message;
import model.MessageAdapter;
import org.apache.kafka.common.serialization.Deserializer;

public class GsonDeserializer implements Deserializer<Message> {

    private final Gson gson = new GsonBuilder().registerTypeAdapter(Message.class, MessageAdapter.class).create();

    @Override
    public Message deserialize(String s, byte[] bytes) {
        return gson.fromJson(new String(bytes), Message.class);
    }
}
