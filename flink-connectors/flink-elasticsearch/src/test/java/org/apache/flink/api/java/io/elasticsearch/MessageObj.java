package org.apache.flink.api.java.io.elasticsearch;

import java.util.Objects;

/**
 * Test object.
 */
public class MessageObj {

	private String user;
	private String message;
	private String inserttime;

	public MessageObj() {
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String getInserttime() {
		return inserttime;
	}

	public void setInserttime(String inserttime) {
		this.inserttime = inserttime;
	}

	@Override
	public String toString() {
		return "MessageObj{" +
			"user='" + user + '\'' +
			", message='" + message + '\'' +
			", inserttime='" + inserttime + '\'' +
			'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		MessageObj that = (MessageObj) o;
		return Objects.equals(user, that.user) &&
			Objects.equals(message, that.message) &&
			Objects.equals(inserttime, that.inserttime);
	}

	@Override
	public int hashCode() {
		return Objects.hash(user, message, inserttime);
	}
}
