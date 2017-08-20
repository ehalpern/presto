package com.facebook.presto.noms.util;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class NgqlType {
    public enum Kind {
        ENUM,
        LIST,
        NON_NULL,
        OBJECT,
        SCALAR,
        UNION
    }

    private final JsonObject object;
    private final boolean nonNull;
    private final Kind kind;
    private final NgqlType ofType;

    /*package*/ NgqlType(JsonObject o) {
        nonNull = o.getString("kind") == Kind.NON_NULL.toString();
        object = nonNull ? o.getJsonObject("ofType") : o;
        kind = Kind.valueOf(object.getString("kind"));
        if (object.getJsonObject("ofType") == null) {
            ofType = null;
        } else {
            ofType = new NgqlType(object.getJsonObject("ofType"));
        }
    }

    public String name() { return object.getString("name"); }

    public Kind kind() { return kind; }

    public NgqlType ofType() { return ofType; }

    public boolean nonNull() { return nonNull; }

    public Map<String, NgqlType> fields() {
        JsonArray fields = object.getJsonArray("fields");
        if (fields == null) {
            return Collections.emptyMap();
        } else {
            Map<String, NgqlType> map = new HashMap<>();

            for (JsonObject f : fields.getValuesAs(JsonObject.class)) {
                map.put(f.getString("name"), new NgqlType(f.getJsonObject("type")));
            }
            return map;
        }
    }

}
