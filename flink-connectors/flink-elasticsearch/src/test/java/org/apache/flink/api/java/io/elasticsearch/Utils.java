package org.apache.flink.api.java.io.elasticsearch;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentType;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static junit.framework.TestCase.assertTrue;

/**
 * Testing utility classes.
 */
public class Utils {
	private static final Logger LOG = LoggerFactory.getLogger(Utils.class);
	private static Random rand = new java.util.Random();
	private static ObjectMapper mapper = new ObjectMapper();

	public static DateTime generateRandomDateBetween(DateTime from, DateTime to) {
		if (from.isAfter(to) || from.isEqual(to)) {
			throw new IllegalArgumentException("from must be before to");
		}

		Duration duration = new Duration(from, to);
		double addedDuration = duration.getMillis() * rand.nextFloat();
		return from.plus(Math.round(addedDuration));
	}

	public static Set<MessageObj> indexTestData(Client client, String indexName, String typeName,
												int numMessages, DateTime from, DateTime to) throws Exception {
		BulkRequestBuilder bulkRequest = client.prepareBulk();

		Set<MessageObj> objs = new HashSet<>();
		for (int i = 0; i < numMessages; i++) {
			MessageObj message = new MessageObj();
			message.setUser("user" + i);
			message.setMessage("user" + i + " message");
			message.setInserttime(Utils.generateRandomDateBetween(from, to).toString());
			objs.add(message);
		}

		for (MessageObj o : objs) {
			LOG.debug("Adding message to ES index: '{}'", o);

			IndexRequestBuilder indexRequestBuilder = client.prepareIndex(indexName, typeName)
				.setSource(mapper.writeValueAsBytes(o), XContentType.JSON);

			bulkRequest.add(indexRequestBuilder);
		}

		BulkResponse bulkItemResponses = bulkRequest.get();

		if (bulkItemResponses.hasFailures()) {
			throw new Exception(String.format("Could not prepare data, '%s'", bulkItemResponses.buildFailureMessage()));
		}

		RefreshResponse refreshResponse = client.admin().indices().prepareRefresh(indexName).get();
		if (refreshResponse.getFailedShards() > 0) {
			throw new Exception(String.format("Could not prepare data, %s shards failed", refreshResponse.getFailedShards()));
		}

		return objs;
	}

	@Test
	public void testGenerateRandomDate() {
		DateTime from = DateTime.parse("2015-01-01T00:00");
		DateTime to = DateTime.parse("2015-01-01T00:01");
		DateTime dateTime = generateRandomDateBetween(from, to);
		System.out.println(dateTime.toDateTimeISO());
		assertTrue(dateTime.isAfter(from) || dateTime.isEqual(from));
		assertTrue(dateTime.isBefore(to) || dateTime.isEqual(to));
	}
}
