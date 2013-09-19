package org.fukata.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import java.util.Map;

@Description(name = "map_value", value = "_FUNC_(map, keys) - extract value from map")
public class MapValue extends UDF {
    @SuppressWarnings("rawtypes")
    Object doEvaluate (Object map, Object...keys) {
        Object value = null;
        for ( Object key : keys ) {
            @SuppressWarnings("unchecked")
            Map<Object, Object> m = (Map<Object, Object>) (value == null ? map : value); 
            if ( !m.containsKey(key) ) return null;

            value = m.get(key);
            if ( null == value ) return null;
        }
        return value.toString();
    }
}
