/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.noms.ngql;

import com.google.common.collect.ImmutableList;

import javax.json.JsonException;
import javax.json.JsonObject;
import javax.json.JsonValue;

import java.util.Iterator;
import java.util.List;

public class NomsResult
{
    private final JsonValue valueAtPath;

    /*package*/ NomsResult(JsonObject json, List<String> path)
    {
        String fullPath = path.isEmpty() ? "/data" : "/data/" + String.join("/", path);
        JsonValue value;
        try {
            value = json.getValue(fullPath);
        }
        catch (JsonException e) {
            value = JsonValue.EMPTY_JSON_ARRAY;
        }
        this.valueAtPath = value;
    }

    /*package*/ int size()
    {
        return valueAtPath.getValueType() == JsonValue.ValueType.ARRAY ?
            valueAtPath.asJsonArray().size() : 1;
    }

    /*package*/ JsonValue value()
    {
        return valueAtPath;
    }

    /*package*/ Iterator<JsonValue> rows()
    {
        if (valueAtPath.getValueType() == JsonValue.ValueType.ARRAY) {
            return valueAtPath.asJsonArray().iterator();
        }
        else {
            return ImmutableList.of(valueAtPath).iterator();
        }
    }

    public String toString()
    {
        return valueAtPath.toString();
    }
}
