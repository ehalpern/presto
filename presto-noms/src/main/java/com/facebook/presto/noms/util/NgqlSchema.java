package com.facebook.presto.noms.util;

import javax.json.JsonObject;
import java.util.HashMap;
import java.util.Map;

public class NgqlSchema {
    private final JsonObject object;
    private final NgqlType rootType;

    private final Map<String, NgqlType> types = new HashMap<>();

    /*package*/ NgqlSchema(JsonObject o) {
        object = o.getJsonObject("data").getJsonObject("__schema");
        for (JsonObject t : object.getJsonArray("types").getValuesAs(JsonObject.class)) {
            NgqlType type = new NgqlType(t);
            types.put(type.name(), type);
        }
        String rootTypeName = object.getJsonObject("queryType").getString("name");
        rootType = types.get(rootTypeName);
    }

    public NgqlType rootValueType() {
        return resolve(rootType.fieldType("root"));
    }

    public NgqlType lastCommitValueType() {
        return resolve(rootValueType().fieldType("value"));
    }

    public NgqlType resolve(NgqlType type) {
        return type.reference() ? types.get(type.name()) : type;
    }
}
