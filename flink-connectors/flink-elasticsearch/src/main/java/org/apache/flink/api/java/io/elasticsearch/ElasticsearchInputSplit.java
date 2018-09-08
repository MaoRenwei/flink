package org.apache.flink.api.java.io.elasticsearch;

import org.apache.flink.core.io.InputSplit;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * An {@link InputSplit} with Elasticsearch specific methods that allow augmenting a query with the
 * {@link InputSplit} parameters.
 *
 * @param <T> Type of InputSplit range (e.g. DateTime)
 */
public class ElasticsearchInputSplit<T> implements InputSplit {

	private T from;
	private T to;
	private String splitFieldName;
	private int splitNum;

	public ElasticsearchInputSplit(int splitNum) {
		this.splitNum = splitNum;
	}

	public ElasticsearchInputSplit(T from, T to, String splitFieldName, int splitNum) {
		this.from = from;
		this.to = to;
		this.splitFieldName = splitFieldName;
		this.splitNum = splitNum;
	}

	@Override
	public int getSplitNumber() {
		return splitNum;
	}

	public QueryBuilder applySplit(QueryBuilder userQuery) {
		QueryBuilder splitQuery = QueryBuilders.matchAllQuery();

		if (from != null && to != null) {
			splitQuery = QueryBuilders.rangeQuery(splitFieldName)
				.gte(from)
				.lte(to);
		}

		return QueryBuilders.boolQuery()
			.must(userQuery)
			.must(splitQuery);
	}
}
