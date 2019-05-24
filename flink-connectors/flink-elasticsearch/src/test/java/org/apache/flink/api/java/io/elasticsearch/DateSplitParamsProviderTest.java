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
	public void testDateSplitInto3() {

		List<DateTime> expectedParamsSplitId0 = Lists.newArrayList(
			DateTime.parse("2014-01-01T00:00:00.0Z"),
			DateTime.parse("2014-09-01T07:59:59.999Z"));

		List<DateTime> expectedParamsSplitId1 = Lists.newArrayList(
			DateTime.parse("2014-09-01T08:00:00.0Z"),
			DateTime.parse("2015-05-02T15:59:59.999Z"));

		List<DateTime> expectedParamsSplitId2 = Lists.newArrayList(
			DateTime.parse("2015-05-02T16:00:00.0Z"),
			DateTime.parse("2016-01-01T00:00:00.0Z"));

		DateSplitParamsProvider params =
			new DateSplitParamsProvider(from, to, "insert_time", 3);

		Assert.assertEquals(3, params.getNumSplits());
		List<DateTime> paramsPerSplit0 = params.getParamsPerSplit(0);
		Assert.assertEquals(expectedParamsSplitId0, paramsPerSplit0);

		List<DateTime> paramsPerSplit1 = params.getParamsPerSplit(1);
		Assert.assertEquals(expectedParamsSplitId1, paramsPerSplit1);

		List<DateTime> paramsPerSplit2 = params.getParamsPerSplit(2);
		Assert.assertEquals(expectedParamsSplitId2, paramsPerSplit2);
	}

	@Test
	public void testDateSplitInto12() {
		List<String> expectedSplitsList = Lists.newArrayList(
			"2014-01-01T00:00:00.000Z", "2014-03-02T19:59:59.999Z",
			"2014-03-02T20:00:00.000Z", "2014-05-02T15:59:59.999Z",
			"2014-05-02T16:00:00.000Z", "2014-07-02T11:59:59.999Z",
			"2014-07-02T12:00:00.000Z", "2014-09-01T07:59:59.999Z",
			"2014-09-01T08:00:00.000Z", "2014-11-01T03:59:59.999Z",
			"2014-11-01T04:00:00.000Z", "2014-12-31T23:59:59.999Z",
			"2015-01-01T00:00:00.000Z", "2015-03-02T19:59:59.999Z",
			"2015-03-02T20:00:00.000Z", "2015-05-02T15:59:59.999Z",
			"2015-05-02T16:00:00.000Z", "2015-07-02T11:59:59.999Z",
			"2015-07-02T12:00:00.000Z", "2015-09-01T07:59:59.999Z",
			"2015-09-01T08:00:00.000Z", "2015-11-01T03:59:59.999Z",
			"2015-11-01T04:00:00.000Z", "2016-01-01T00:00:00.000Z");

		DateSplitParamsProvider params =
			new DateSplitParamsProvider(from, to, "insert_time", expectedSplitsList.size() / 2);
		Assert.assertEquals(expectedSplitsList.size() / 2, params.getNumSplits());

		for (int i = 0; i < expectedSplitsList.size(); i += 2) {
			List<DateTime> expectedParamsSplit = Lists.newArrayList(
				DateTime.parse(expectedSplitsList.get(i)),
				DateTime.parse(expectedSplitsList.get(i + 1)));

			List<DateTime> paramsPerSplit = params.getParamsPerSplit(i / 2);
			Assert.assertEquals(expectedParamsSplit, paramsPerSplit);
		}
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
