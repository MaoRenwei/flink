package org.apache.flink.api.java.io.elasticsearch;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.Netty3Plugin;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Input format to read from Elasticsearch.
 * <p>
 * For parallel execution {@link ElasticsearchInputFormat} supports splitting the query into ranges
 * using {@link SplitParamsProvider}.
 * </p>
 * <p>Example:</p>
 * <pre>{@code
 *
 *  ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
 *  QueryBuilder query = QueryBuilders.matchAllQuery();
 *  TypeInformation<SomeObject> typeInformation = PojoTypeInfo.of(SomeObject.class);
 *  EsJsonMapper<SomeObject> esJsonMapper = new EsJsonMapper<>(typeInformation);
 *  SplitParamsProvider<DateTime> paramValuesProvider = new DateSplitParamsProvider(
 * 		DateTime.parse("2014-11-01T00:00"),
 *		DateTime.parse("2016-11-01T00:00"),
 *		"insert_time", 6
 *		);
 *
 *	elasticsearchInputFormat = ElasticsearchInputFormat.builder(hostList, query, esJsonMapper, typeInformation)
 *		.setParametersProvider(paramValuesProvider)
 *		.setIndex("flink-1")
 *		.build();
 *  DataSet<MessageObj> input = env.createInput(elasticsearchInputFormat);
 *  List<MessageObj> list = input.collect();
 *  list.forEach(e -> LOG.info("Result: {}", e));
 *
 * }</pre>
 *
 * @param <OUT> Type of output values
 */
public class ElasticsearchInputFormat<OUT> extends RichInputFormat<OUT, ElasticsearchInputSplit> implements ResultTypeQueryable<OUT> {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchInputFormat.class);

	private int scrollTimeoutMs = 60000;
	private String clusterName = "elasticsearch";
	private int querySize = 100;

	private Function<String, OUT> esTypeMapper;
	private String userEsQuery;
	private String indexName;
	private SplitParamsProvider splitParamsProvider;

	/**
	 * User-provided transport addresses.
	 * <p></p>
	 * <p>We are using {@link InetSocketAddress} because {@link TransportAddress} is not
	 * serializable in Elasticsearch 5.x.</p>
	 */
	private List<InetSocketAddress> hostList;

	private boolean hasNext;
	private int currHitIdx;

	private TransportClient client;
	private TypeInformation<OUT> typeInfo;

	private transient SearchResponse searchScrollResponse;
	private transient SearchHit[] searchHits;
	private transient SearchScrollRequestBuilder scrollRequestBuilder;
	private transient String scrollId;

	public ElasticsearchInputFormat(List<InetSocketAddress> hostList, String query,
									Function<String, OUT> esTypeMapper,
									TypeInformation<OUT> typeInfo) {
		this.hostList = hostList;
		this.userEsQuery = query;
		this.esTypeMapper = esTypeMapper;
		this.typeInfo = typeInfo;
	}

	public static List<TransportAddress> convertInetSocketAddresses(
		List<InetSocketAddress> inetSocketAddresses) {
		if (inetSocketAddresses == null) {
			return null;
		} else {
			List<TransportAddress> converted;
			converted = new ArrayList<>(inetSocketAddresses.size());
			for (InetSocketAddress address : inetSocketAddresses) {
				converted.add(new InetSocketTransportAddress(address));
			}
			return converted;
		}
	}

	public static <G> ElasticsearchInputFormatBuilder<G> builder(
		List<InetSocketAddress> hostList,
		QueryBuilder query,
		Function<String, G> esMapper,
		TypeInformation<G> typeInformation) {

		return new ElasticsearchInputFormatBuilder<>(hostList, query, esMapper, typeInformation);
	}

	// Note that configure is called before createInputSplits as opposed to
	// openInputFormat. So if createInputSplits needs some connection is should be
	// opened here and not in openInputFormat
	// The connection is opened in this method and closed in {@link #closeInputFormat()}.
	@Override
	public void configure(Configuration parameters) {
	}

	private TransportClient createClient(Map<String, String> clientConfig) throws InterruptedException {
		Settings settings = Settings.builder()
			.put(clientConfig)
			.put(NetworkModule.HTTP_TYPE_KEY, Netty3Plugin.NETTY_HTTP_TRANSPORT_NAME)
			.put(NetworkModule.TRANSPORT_TYPE_KEY, Netty3Plugin.NETTY_TRANSPORT_NAME)
			.build();

		TransportClient transportClient = new PreBuiltTransportClient(settings);
		for (TransportAddress transport : convertInetSocketAddresses(hostList)) {
			transportClient.addTransportAddress(transport);
		}

		// verify that we actually are connected to a cluster
		if (transportClient.connectedNodes().isEmpty()) {
			throw new RuntimeException("Elasticsearch client is not connected to any Elasticsearch nodes!");
		}

		if (LOG.isInfoEnabled()) {
			LOG.info("Created Elasticsearch TransportClient with connected nodes {}", transportClient.connectedNodes());
		}

		return transportClient;
	}

	@Override
	public void openInputFormat() {

		HashMap<String, String> clientConfig = new HashMap<>();
		clientConfig.put("cluster.name", clusterName);
		try {
			client = createClient(clientConfig);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void closeInputFormat() {

		if (client != null) {
			client.close();
		}

		client = null;
		splitParamsProvider = null;
	}

	@Override
	public void open(ElasticsearchInputSplit split) {

		QueryBuilder userEsQueryBuilder = QueryBuilders.wrapperQuery(userEsQuery);
		QueryBuilder splitQuery = split.applySplit(userEsQueryBuilder);

		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexName)
			.setScroll(new TimeValue(scrollTimeoutMs))
			.setQuery(splitQuery)
			.setSize(querySize);

		if (LOG.isDebugEnabled()) {
			LOG.debug("[ES] Search request: '{}'", searchRequestBuilder);
		}

		searchScrollResponse = searchRequestBuilder.get();

		scrollId = searchScrollResponse.getScrollId();

		scrollRequestBuilder = client
			.prepareSearchScroll(scrollId)
			.setScroll(new TimeValue(scrollTimeoutMs));

		searchHits = searchScrollResponse.getHits().getHits();
		hasNext = searchHits.length != 0;
		currHitIdx = 0;

		if (LOG.isDebugEnabled()) {
			LOG.debug("[ES] Executed ES Query: '{}'", splitQuery.toString());
			LOG.debug("[ES] Executed ES Query: '{}'", splitQuery);
			LOG.debug("[ES] Got '{}' results", searchHits.length);
		}

	}

	@Override
	public void close() {
		searchScrollResponse = null;
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
		return cachedStatistics;
	}

	@Override
	public ElasticsearchInputSplit[] createInputSplits(int minNumSplits) {
		if (minNumSplits < 1) {
			throw new IllegalArgumentException("Number of input splits has to be at least 1.");
		}

		if (splitParamsProvider == null) {
			return new ElasticsearchInputSplit[]{new ElasticsearchInputSplit(1)};
		}

		int numSplits = Math.max(minNumSplits, splitParamsProvider.getNumSplits());
		if (minNumSplits > splitParamsProvider.getNumSplits()) {
			LOG.info("Num of splits cannot be lower then '{}' (Currently '{}') - " +
				"Overriding", minNumSplits, splitParamsProvider.getNumSplits());
			splitParamsProvider.overrideNumSplits(numSplits);
		}

		ElasticsearchInputSplit[] ret = new ElasticsearchInputSplit[numSplits];
		for (int i = 0; i < ret.length; i++) {
			List paramsPerSplit = splitParamsProvider.getParamsPerSplit(i);
			Object from = paramsPerSplit.get(0);
			Object to = paramsPerSplit.get(1);
			ret[i] = new ElasticsearchInputSplit<>(from, to, splitParamsProvider
				.getSplitFieldName(), i);
			LOG.debug("Creating split from '{}' to '{}'", from, to);
		}

		LOG.info("Created " + ret.length + " splits");

		return ret;
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(ElasticsearchInputSplit[] inputSplits) {
		return new DefaultInputSplitAssigner(inputSplits);
	}

	@Override
	public boolean reachedEnd() {
		return !hasNext;
	}

	@Override
	public OUT nextRecord(OUT rowReuse) {
		if (!hasNext) {
			return null;
		}

		SearchHit hit = searchHits[currHitIdx];
		if (LOG.isDebugEnabled()) {
			LOG.debug("ES Hit '{}'", hit.getSourceAsString());
		}

		rowReuse = esTypeMapper.apply(hit.getSourceAsString());

		currHitIdx++;

		boolean currScrollPageEnded = (currHitIdx == searchHits.length);
		if (currScrollPageEnded) {
			searchScrollResponse = scrollRequestBuilder
				.execute()
				.actionGet();
			searchHits = searchScrollResponse.getHits().getHits();
			currHitIdx = 0;

			if (LOG.isDebugEnabled()) {
				LOG.debug("ES Current scroll ended '{}', fetching next page", scrollId);
			}
		}

		hasNext = currHitIdx != searchHits.length;

		return rowReuse;
	}

	@Override
	public TypeInformation<OUT> getProducedType() {
		return typeInfo;
	}

	/**
	 * Builder for {@link ElasticsearchInputFormat}.
	 */
	public static class ElasticsearchInputFormatBuilder<V> {
		private final ElasticsearchInputFormat<V> inputFormat;

		public ElasticsearchInputFormatBuilder(List<InetSocketAddress> hostList, QueryBuilder query, Function<String, V> esMapper, TypeInformation<V> typeInformation) {
			this.inputFormat = new ElasticsearchInputFormat<>(hostList, query.toString(), esMapper, typeInformation);
		}

		public ElasticsearchInputFormatBuilder<V> setIndex(String indexName) {
			inputFormat.indexName = indexName;
			return this;
		}

		public ElasticsearchInputFormatBuilder<V> setScrollTimeout(int scrollTimeoutMs) {
			inputFormat.scrollTimeoutMs = scrollTimeoutMs;
			return this;
		}

		public ElasticsearchInputFormatBuilder<V> setQuerySize(int querySize) {
			inputFormat.querySize = querySize;
			return this;
		}

		public ElasticsearchInputFormatBuilder<V> setClusterName(String clusterName) {
			inputFormat.clusterName = clusterName;
			return this;
		}

		public ElasticsearchInputFormatBuilder<V> setParametersProvider(
			SplitParamsProvider parameterValuesProvider) {
			inputFormat.splitParamsProvider = parameterValuesProvider;
			return this;
		}

		public ElasticsearchInputFormat<V> build() {
			return inputFormat;
		}
	}
}
