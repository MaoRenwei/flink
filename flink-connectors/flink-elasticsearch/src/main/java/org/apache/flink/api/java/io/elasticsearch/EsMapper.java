package org.apache.flink.api.java.io.elasticsearch;

import java.io.Serializable;
import java.util.Map;
import java.util.function.BiConsumer;

public class EsMapper implements BiConsumer<Map<String, Object>, MessageObj>, Serializable {

	private static final long serialVersionUID = 1L;

	@Override
	public void accept(Map<String, Object> stringObjectMap, MessageObj o) {
		o.setUser((String)stringObjectMap.get("user"));
		o.setMessage((String)stringObjectMap.get("message"));
	}

}
