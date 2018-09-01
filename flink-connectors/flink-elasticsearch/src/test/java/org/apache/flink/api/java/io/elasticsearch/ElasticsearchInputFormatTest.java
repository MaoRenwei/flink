package org.apache.flink.api.java.io.elasticsearch;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import org.apache.flink.types.StringValue;
import org.apache.http.HttpHost;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Testing TODO.
 */
public class ElasticsearchInputFormatTest {
	private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchInputFormatTest.class);

	private ElasticsearchInputFormat<MessageObj> elasticsearchInputFormat;
	private List<InetSocketAddress> localhostList = new ArrayList<>();

	@Before
	public void before() {
		localhostList.add(new InetSocketAddress("localhost", 9300));
	}

//	@Test
//	public void testBasic() throws IOException {
//		elasticsearchInputFormat = ElasticsearchInputFormat.buildElasticsearchInputFormat()
//			.setTransportAddresses(localhostList)
//			.build();
//
//		elasticsearchInputFormat.openInputFormat();
//
//	}

	@Test
	public void testBasic2() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		TypeInformation<MessageObj> typeInformation = PojoTypeInfo.of(MessageObj.class);

		QueryBuilder query = QueryBuilders.matchAllQuery();

		EsMapper esMapper = new EsMapper();

		elasticsearchInputFormat = ElasticsearchInputFormat.buildElasticsearchInputFormat(esMapper)
			.setTransportAddresses(localhostList)
			.setTypeInfo(typeInformation)
			.setQueryBuilder(query)
			.setIndex("flink-1")
			.build();

		DataSet<MessageObj> input = env.createInput(elasticsearchInputFormat).setParallelism(1);
		List<MessageObj> list = input.collect();

		list.forEach(e -> LOG.info("Result: {}", e));
	}
}
