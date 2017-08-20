package com.facebook.presto.noms.util;

import javax.json.JsonObject;
import java.util.HashMap;
import java.util.Map;

public class NgqlSchema {
    private final JsonObject object;
    private final NgqlType rootType;
    private final NgqlType rootValueType;

    private final Map<String, NgqlType> types = new HashMap<>();

    /*package*/ NgqlSchema(JsonObject o) {
        object = o.getJsonObject("/data/__schema");
        for (JsonObject t : object.getJsonArray("types").getValuesAs(JsonObject.class)) {
            NgqlType type = new NgqlType(t);
            types.put(type.name(), type);
        }
        String rootTypeName = object.getString("/queryType/name");
        rootType = types.get(rootTypeName);
        rootValueType = types.get(rootType.fields().get("value").name());
    }

    public NgqlType rootType() { return rootType; }
    public NgqlType rootValueType() { return rootValueType; }
    public Map<String, NgqlType> types() { return types; }
}
