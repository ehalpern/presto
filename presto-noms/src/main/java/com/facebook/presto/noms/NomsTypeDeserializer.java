package com.facebook.presto.noms;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import io.airlift.json.JsonCodec;

import java.io.IOException;

import static io.airlift.json.JsonCodec.jsonCodec;

public class NomsTypeDeserializer extends JsonDeserializer<NomsType> {
    private final JsonCodec<DerivedNomsType> actualCodec = jsonCodec(DerivedNomsType.class);

    @Override
    public NomsType deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
        ObjectCodec oc = jsonParser.getCodec();
        JsonNode node = oc.readTree(jsonParser);
        boolean hasArguments = node.has("arguments");
        boolean hasFields = node.has("fields");
        if (hasArguments || hasFields) {
            return actualCodec.fromJson(node.toString());
        } else {
            return RootNomsType.valueOf(node.textValue());
        }
    }
}
