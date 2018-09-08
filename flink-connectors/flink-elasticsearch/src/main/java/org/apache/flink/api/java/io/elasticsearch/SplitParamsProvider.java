package org.apache.flink.api.java.io.elasticsearch;

import org.apache.flink.core.io.InputSplit;

import java.io.Serializable;
import java.util.List;

/**
 * Provides an API for accessing {@link InputSplit} parameters.
 * <p>Implement {@link SplitParamsProvider} to provide needed parameters for defining an {@link InputSplit}
 * during the {@link org.apache.flink.api.common.io.InputFormat#createInputSplits(int)} method.
 * </p>
 *
 * @param <T> Range paramter type which we'd like to split using the provider
 */
public interface SplitParamsProvider<T> extends Serializable {

	List<T> getParamsPerSplit(int splitId);

	String getSplitFieldName();

	void overrideNumSplits(int numSplits);

	int getNumSplits();
}
