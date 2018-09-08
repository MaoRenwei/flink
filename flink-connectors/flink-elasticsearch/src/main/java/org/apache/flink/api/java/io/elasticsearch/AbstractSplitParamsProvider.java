package org.apache.flink.api.java.io.elasticsearch;

import java.util.List;

/**
 * Implements {@link SplitParamsProvider} with range defined between <i>from</i> and <i>to</i>.
 * Each split will get an equal amount of values in the range [from, to].
 * @param <T>
 */
public class AbstractSplitParamsProvider<T> implements SplitParamsProvider<T> {

	protected final T from;
	protected final T to;
	protected final String fieldName;
	protected int numSplits;

	public AbstractSplitParamsProvider(T from, T to, String fieldName) {
		this.from = from;
		this.to = to;
		this.fieldName = fieldName;
		this.numSplits = 1;
	}

	public AbstractSplitParamsProvider(T from, T to, String fieldName, int numSplits) {
		this(from, to, fieldName);
		this.numSplits = numSplits;
	}

	@Override
	public List<T> getParamsPerSplit(int splitId) {
		return null;
	}

	@Override
	public String getSplitFieldName() {
		return fieldName;
	}

	@Override
	public void overrideNumSplits(int numSplits) {
		this.numSplits = numSplits;
	}

	@Override
	public int getNumSplits() {
		return numSplits;
	}
}
