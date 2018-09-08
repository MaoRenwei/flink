package org.apache.flink.api.java.io.elasticsearch;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.function.Function;

/**
 * Map JSON string to a generic object.
 * @param <T> Object type to map JSON string to
 */
public class EsJsonMapper<T> implements Function<String, T>, Serializable {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(EsJsonMapper.class);

	private final ObjectReader objectReader;

	public EsJsonMapper(TypeInformation<T> typeInformation) {
		ObjectMapper objectMapper = new ObjectMapper();
		this.objectReader = objectMapper.readerFor(typeInformation.getTypeClass());
	}

	@Override
	public T apply(String s) {

		T messageObj = null;
		try {
			messageObj = objectReader.readValue(s);
		} catch (IOException e) {
			LOG.warn("Cannot map value '{}'/'{}'", s, e);
		}

		return messageObj;
	}
}
