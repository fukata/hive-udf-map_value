package org.fukata.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name = "map_value_string", value = "_FUNC_(map, keys) - extract value from map")
public class MapValueString extends MapValue {
    public String evaluate (Object map, Object...keys) {
        Object value = doEvaluate(map, keys);
        return value == null ? null : value.toString();
    }
}
