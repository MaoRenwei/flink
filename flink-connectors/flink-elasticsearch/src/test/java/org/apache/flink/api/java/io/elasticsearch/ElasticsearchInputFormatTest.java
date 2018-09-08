package org.apache.flink.api.java.io.elasticsearch;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.InstantiationUtil;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertFalse;

/**
 * Testing ElasticsearchInputFormat.
 */
public class ElasticsearchInputFormatTest extends AbstractTestBase {
	private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchInputFormatTest.class);

	private static final String ES_TEST_CLUSTER_NAME = "flink-test-cluster";
	private static final String ES_INDEX_NAME = "flink-test-index";
	private static final String ES_TYPE_NAME = "flink_t";

	private static final String ES_INDEX_NAME_SINGLE = ES_INDEX_NAME + "-single";
	private static final String ES_INDEX_NAME_MULTI = ES_INDEX_NAME + "-multi";

	private QueryBuilder query = QueryBuilders.matchAllQuery();
	private TypeInformation<MessageObj> typeInformation = PojoTypeInfo.of(MessageObj.class);
	private EsJsonMapper<MessageObj> esJsonMapper = new EsJsonMapper<>(typeInformation);

	private static EmbeddedEs embeddedEs;

	private static Set<MessageObj> expectedSingleMessage;
	private static Set<MessageObj> expectedMultiMessage;

	private static DateTime from = DateTime.parse("2014-11-01T00:00");
	private static DateTime to = DateTime.parse("2019-11-01T00:00");

	private SplitParamsProvider<DateTime> paramValuesProvider = new
		DateSplitParamsProvider(
		from,
		to,
		"inserttime", 6
	);

	private List<InetSocketAddress> localhostList = Lists.newArrayList(new InetSocketAddress("localhost", 9300));

	@BeforeClass
	public static void prepare() throws Exception {

		LOG.info("-------------------------------------------------------------------------");
		LOG.info("    Starting embedded Elasticsearch node ");
		LOG.info("-------------------------------------------------------------------------");

		embeddedEs = new EmbeddedEs();
		embeddedEs.start(TEMPORARY_FOLDER.newFolder(), ES_TEST_CLUSTER_NAME);

		Client client = embeddedEs.getClient();
		expectedSingleMessage = Utils.indexTestData(client, ES_INDEX_NAME_SINGLE, ES_TYPE_NAME, 1,
			from, to);

		expectedMultiMessage = Utils.indexTestData(client, ES_INDEX_NAME_MULTI, ES_TYPE_NAME, 100,
			from, to);
	}

	@AfterClass
	public static void shutdown() throws Exception {

		LOG.info("-------------------------------------------------------------------------");
		LOG.info("    Shutting down embedded Elasticsearch node ");
		LOG.info("-------------------------------------------------------------------------");

		embeddedEs.close();
	}

	@Test
	public void testSingleMessageFullFlow() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		ElasticsearchInputFormat<MessageObj> elasticsearchInputFormat = ElasticsearchInputFormat.builder(
			localhostList, query, esJsonMapper, typeInformation)
			.setParametersProvider(paramValuesProvider)
			.setIndex(ES_INDEX_NAME_SINGLE)
			.setClusterName(ES_TEST_CLUSTER_NAME)
			.build();

		DataSet<MessageObj> input = env.createInput(elasticsearchInputFormat);

		assertEquals(expectedSingleMessage, Sets.newHashSet(input.collect()));
	}

	@Test
	public void testSerialization() throws IOException, ClassNotFoundException {

		SplitParamsProvider<DateTime> params = new
			DateSplitParamsProvider(
			from,
			to,
			"inserttime", 1
		);

		ElasticsearchInputFormat<MessageObj> elasticsearchInputFormat = ElasticsearchInputFormat.builder(
			localhostList, query, esJsonMapper, typeInformation)
			.setParametersProvider(params)
			.setClusterName(ES_TEST_CLUSTER_NAME)
			.setIndex(ES_INDEX_NAME_SINGLE)
			.build();

		byte[] bytes = InstantiationUtil.serializeObject(elasticsearchInputFormat);
		ElasticsearchInputFormat<MessageObj> copy = InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());

		ElasticsearchInputSplit[] inputSplits = copy.createInputSplits(1);
		copy.openInputFormat();
		copy.open(inputSplits[0]);
		assertFalse(copy.reachedEnd());

		MessageObj messageObj = copy.nextRecord(null);
		assertNotNull(messageObj);
		assertEquals(expectedSingleMessage, Sets.newHashSet(messageObj));
	}

	@Test
	public void testReadAllSplits() throws IOException {

		SplitParamsProvider<DateTime> params = new
			DateSplitParamsProvider(
			from,
			to,
			"inserttime", 4
		);

		ElasticsearchInputFormat<MessageObj> elasticsearchInputFormat = ElasticsearchInputFormat.builder(
			localhostList, query, esJsonMapper, typeInformation)
			.setParametersProvider(params)
			.setClusterName(ES_TEST_CLUSTER_NAME)
			.setIndex(ES_INDEX_NAME_MULTI)
			.build();

		ElasticsearchInputSplit[] splits = elasticsearchInputFormat.createInputSplits(4);
		assertEquals(4, splits.length);
		elasticsearchInputFormat.openInputFormat();

		long cnt = 0;
		Set<MessageObj> results = new HashSet<>();
		// read all splits
		for (ElasticsearchInputSplit split : splits) {

			// open split
			elasticsearchInputFormat.open(split);

			// read and count all rows
			while (!elasticsearchInputFormat.reachedEnd()) {
				MessageObj object = elasticsearchInputFormat.nextRecord(null);
				assertNotNull(object);
				results.add(object);
				cnt++;
			}
		}
		// check that all rows have been read
		assertEquals(100, cnt);
		assertEquals(expectedMultiMessage, results);
	}
}
