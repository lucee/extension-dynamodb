package org.lucee.extension.aws.dynamodb;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.lucee.extension.aws.dynamodb.util.CommonUtil;

import lucee.commons.io.log.Log;
import lucee.loader.util.Util;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

public class AmazonDynamoDBClient {

	private static final String DEFAULT_HOST = "dynamodb.amazonaws.com";
	private static Map<String, AmazonDynamoDBClient> pool = new ConcurrentHashMap<>();

	private DynamoDbClient client;
	private Log log;
	private long created;
	private String accessKeyId;
	private String secretAccessKey;
	private String host;
	private long liveTimeout;
	private String region;

	public static DynamoDbClient get(String accessKeyId, String secretAccessKey, String host, String region, long liveTimeout, Log log) {
		String key = accessKeyId + ":" + secretAccessKey + ":" + host + ":" + region;

		if (log != null) {
			log.debug("DynamoDBClient", "Requesting DynamoDB client for region: " + region + ", host: " + host);
		}

		AmazonDynamoDBClient client = pool.get(key);
		if (client == null || client.isExpired()) {
			synchronized (CommonUtil.createToken("AmazonDynamoDBClient", key)) {
				if (client == null || client.isExpired()) {
					if (log != null) {
						if (client == null) {
							log.info("DynamoDBClient", "Creating new DynamoDB client for region: " + region);
						}
						else {
							log.info("DynamoDBClient", "Existing DynamoDB client expired, creating new one for region: " + region);
						}
					}
					pool.put(key, client = new AmazonDynamoDBClient(accessKeyId, secretAccessKey, host, region, liveTimeout, log));
				}
			}

		}
		else {
			if (log != null) {
				log.debug("DynamoDBClient", "Reusing existing DynamoDB client for region: " + region);
			}
		}

		return client.getDynamoDbClient();
	}

	private AmazonDynamoDBClient(String accessKeyId, String secretAccessKey, String host, String region, long liveTimeout, Log log) {
		this.accessKeyId = accessKeyId;
		this.secretAccessKey = secretAccessKey;
		this.host = host;
		this.region = region;
		this.log = log;
		this.liveTimeout = liveTimeout;
		this.created = System.currentTimeMillis();

		if (log != null) {
			log.info("DynamoDBClient", "Initializing DynamoDB client with region: " + region + ", host: " + host + ", liveTimeout: " + liveTimeout + "ms");
		}

		client = create();

		if (log != null) {
			log.info("DynamoDBClient", "Successfully created DynamoDB client");
		}
	}

	public DynamoDbClient create() {
		DynamoDbClientBuilder builder = DynamoDbClient.builder();

		// Set credentials
		builder.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey)));

		// Set region with fallback
		String effectiveRegion = !Util.isEmpty(region, true) ? region.trim() : "us-east-1";

		if (log != null) {
			log.debug("DynamoDBClient", "Using region: " + effectiveRegion + " (original: " + region + ")");
		}

		builder.region(Region.of(effectiveRegion));

		// Handle custom host/endpoint
		if (!Util.isEmpty(host, true) && !host.trim().equalsIgnoreCase(DEFAULT_HOST)) {
			String effectiveHost = host.trim();

			// Ensure the host has a protocol
			if (!effectiveHost.startsWith("http://") && !effectiveHost.startsWith("https://")) {
				effectiveHost = "https://" + effectiveHost;
			}

			if (log != null) {
				log.info("DynamoDBClient", "Using custom endpoint: " + effectiveHost);
			}

			builder.endpointOverride(URI.create(effectiveHost));
		}
		else {
			if (log != null) {
				log.debug("DynamoDBClient", "Using standard AWS endpoint for region: " + effectiveRegion);
			}
		}

		return builder.build();
	}

	private boolean isExpired() {
		return (created + liveTimeout) < System.currentTimeMillis();
	}

	public DynamoDbClient getDynamoDbClient() {
		return client;
	}

	public void release() {
		if (log != null) {
			log.debug("DynamoDBClient", "Releasing DynamoDB client resources");
		}
		if (client != null) {
			client.close(); // SDK v2 clients should be closed
		}
	}
}