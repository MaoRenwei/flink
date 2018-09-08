package org.apache.flink.api.java.io.elasticsearch;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.InternalSettingsPreparer;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.Netty3Plugin;

import java.io.File;
import java.util.Collections;

/**
 * Embedded Elasticsearch class for end to end tests.
 */
public class EmbeddedEs {
	private Node node;

	public void start(File tmpDataFolder, String clusterName) throws Exception {
		if (node == null) {
			Settings settings = Settings.builder()
				.put("cluster.name", clusterName)
				.put("http.enabled", false)
				.put("path.home", tmpDataFolder.getParent())
				.put("path.data", tmpDataFolder.getAbsolutePath())
				.put(NetworkModule.HTTP_TYPE_KEY, Netty3Plugin.NETTY_HTTP_TRANSPORT_NAME)
				.put(NetworkModule.TRANSPORT_TYPE_KEY, Netty3Plugin.NETTY_TRANSPORT_NAME)
				.build();

			node = new PluginNode(settings);
			node.start();
		}
	}

	public void close() throws Exception {
		if (node != null && !node.isClosed()) {
			node.close();
			node = null;
		}
	}

	public Client getClient() {
		if (node != null && !node.isClosed()) {
			return node.client();
		} else {
			return null;
		}
	}

	private static class PluginNode extends Node {
		public PluginNode(Settings settings) {
			super(InternalSettingsPreparer.prepareEnvironment(settings, null), Collections.<Class<? extends Plugin>>singletonList(Netty3Plugin.class));
		}
	}
}
