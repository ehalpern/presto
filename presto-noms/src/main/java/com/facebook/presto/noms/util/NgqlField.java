package com.facebook.presto.noms.util;

import javax.json.JsonObject;

public class NgqlField {
    private final JsonObject object;
    private final String name;
    private final NgqlType type;

    private NgqlField(JsonObject o) {
        object = o;
        name = o.getString("name");
        type = new NgqlType(o.getJsonObject("type"));
    }

    public String name() { return name; }
    public NgqlType type() { return type; }
}
