package org.apache.flink.api.java.io.elasticsearch;

import org.joda.time.DateTime;
import org.joda.time.Duration;

import java.util.ArrayList;
import java.util.List;

/**
 * A Date based InputSplit parameters provider.
 * <p>
 * Can be used to extract equal length ranges between a start date and an end date.
 * </p>
 *
 * <pre>{@code
 * SplitParamsProvider<DateTime> paramValuesProvider = new DateSplitParamsProvider(
 *	 DateTime.parse("2014-11-01T00:00"),
 *	 DateTime.parse("2016-11-01T00:00"),
 *	 "insert_time", 6);
 * }</pre>
 *
 */
public class DateSplitParamsProvider extends AbstractSplitParamsProvider<DateTime> {
	private static final long serialVersionUID = 1L;
	private final Duration duration;

	public DateSplitParamsProvider(DateTime from, DateTime to, String fieldName) {
		super(from, to, fieldName);
		this.duration = new Duration(from, to);
	}

	public DateSplitParamsProvider(DateTime from, DateTime to, String fieldName, int numSplits) {
		this(from, to, fieldName);
		this.numSplits = numSplits;
	}

	@Override
	public List<DateTime> getParamsPerSplit(int splitId) {
		if (splitId >= numSplits) {
			throw new IllegalArgumentException("Required splitId '{}' is out of range");
		}

		Duration durationPerFetch = duration.dividedBy(numSplits);

		List<DateTime> params = new ArrayList<>();
		DateTime splitFrom = from.plus(durationPerFetch.multipliedBy(splitId));

		DateTime splitTo;
		if (splitId + 1 == numSplits) {
			splitTo = from.plus(durationPerFetch.multipliedBy(splitId + 1));
		}
		else {
			splitTo = from.plus(durationPerFetch.multipliedBy(splitId + 1)).minus(1);
		}

		params.add(splitFrom);
		params.add(splitTo);

		return params;
	}
}
