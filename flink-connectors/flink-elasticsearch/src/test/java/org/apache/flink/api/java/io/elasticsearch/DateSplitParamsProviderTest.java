package org.apache.flink.api.java.io.elasticsearch;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Testing DateSplitParam provider.
 */
public class DateSplitParamsProviderTest {
	private DateTime from = DateTime.parse("2014-01-01T00:00:00.0Z");
	private DateTime to = DateTime.parse("2016-01-01T00:00:00.0Z");

	@Test
	public void testDateSplitInto2() {

		List<DateTime> expectedParamsSplitId0 = Lists.newArrayList(
			DateTime.parse("2014-01-01T00:00:00.0Z"),
			DateTime.parse("2014-12-31T23:59:59.999Z"));

		List<DateTime> expectedParamsSplitId1 = Lists.newArrayList(
			DateTime.parse("2015-01-01T00:00:00.0Z"),
			DateTime.parse("2016-01-01T00:00:00.0Z"));

		DateSplitParamsProvider params =
			new DateSplitParamsProvider(from, to, "insert_time", 2);

		Assert.assertEquals(params.getNumSplits(), 2);
		List<DateTime> paramsPerSplit0 = params.getParamsPerSplit(0);
		Assert.assertEquals(expectedParamsSplitId0, paramsPerSplit0);

		List<DateTime> paramsPerSplit1 = params.getParamsPerSplit(1);
		Assert.assertEquals(expectedParamsSplitId1, paramsPerSplit1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDateSplitInvalidSplit() {

		DateSplitParamsProvider params =
			new DateSplitParamsProvider(from, to, "insert_time", 2);

		params.getParamsPerSplit(4);
	}

	@Test
	public void testDateSplitNoSplitArg() {
		DateSplitParamsProvider params =
			new DateSplitParamsProvider(from, to, "insert_time");

		Assert.assertEquals(1, params.getNumSplits());
		Assert.assertEquals(Lists.newArrayList(from, to), params.getParamsPerSplit(0));
	}
}
