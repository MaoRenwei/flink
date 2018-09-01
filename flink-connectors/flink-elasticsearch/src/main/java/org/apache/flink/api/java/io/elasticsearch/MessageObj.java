package org.apache.flink.api.java.io.elasticsearch;

import java.util.Objects;

public class MessageObj {

	String user;
	String message;


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

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		MessageObj that = (MessageObj) o;
		return Objects.equals(user, that.user) &&
			Objects.equals(message, that.message);
	}

	@Override
	public int hashCode() {

		return Objects.hash(user, message);
	}

	@Override
	public String toString() {
		return "MessageObj{" +
			"user='" + user + '\'' +
			", message='" + message + '\'' +
			'}';
	}
}
