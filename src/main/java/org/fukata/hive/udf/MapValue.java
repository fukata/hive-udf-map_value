package org.fukata.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import java.util.Map;

@Description(name = "map_value", value = "_FUNC_ extract value from map", extended = "")
public class MapValue extends UDF {
    @SuppressWarnings("rawtypes")
    //public String evaluate (Map map, Object...keys) {
    public String evaluate (Map<String, Map<String, String>> map, String...keys) {
        Object value = null;
        for ( Object key : keys ) {
            @SuppressWarnings("unchecked")
            Map<Object, Object> m = (Map<Object, Object>) (value == null ? map : value); 
            if ( !m.containsKey(key) ) return null;

            value = m.get(key);
            if ( null == value ) return null;
            
            String clazzName = value.getClass().getName();
            if ( "java.lang.String".equals(clazzName) ) {
                return value.toString();
            }
        }

        String clazzName = value.getClass().getName();
        if ( "java.lang.String".equals(clazzName) ) {
            return value.toString();
        }

        return value.toString();
    }
}
