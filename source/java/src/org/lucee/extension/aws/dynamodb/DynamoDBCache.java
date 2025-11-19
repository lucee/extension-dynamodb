package org.lucee.extension.aws.dynamodb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.lucee.extension.aws.dynamodb.util.Coder;

import lucee.commons.io.cache.CacheEntry;
import lucee.commons.io.cache.CacheEntryFilter;
import lucee.commons.io.cache.CacheKeyFilter;
import lucee.commons.io.log.Log;
import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.loader.util.Util;
import lucee.runtime.config.Config;
import lucee.runtime.exp.PageException;
import lucee.runtime.type.Array;
import lucee.runtime.type.Struct;
import lucee.runtime.util.Cast;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTimeToLiveRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTimeToLiveResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexDescription;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveSpecification;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateTimeToLiveRequest;

public class DynamoDBCache extends CacheSupport {
	private static final Map<String, String> TTL_ATTR_NAMES = Map.of("#ttl", "ttl");

	private String accessKeyId;
	private String secretAccessKey;
	private String tableName;
	private String host;
	private String region;
	private long liveTimeout;
	private Log log;
	private CFMLEngine eng;

	@Override
	public void init(Config config, String cacheName, Struct arguments) throws IOException {
		init(config, arguments);
	}

	public void init(Struct arguments) throws IOException {
		init(null, arguments);
	}

	public void init(Config config, Struct arguments) throws IOException {
		// this.cl = arguments.getClass().getClassLoader();

		eng = CFMLEngineFactory.getInstance();

		if (config == null) config = eng.getThreadConfig();

		Cast caster = eng.getCastUtil();

		// tableName
		if (Util.isEmpty(tableName, true)) tableName = caster.toString(arguments.get("tableName", null), null);
		if (Util.isEmpty(tableName, true)) tableName = caster.toString(arguments.get("table", null), null);
		if (Util.isEmpty(tableName, true)) {
			throw new IOException("tableName is required for DynamoDB cache");
		}

		// accessKeyId
		if (Util.isEmpty(accessKeyId, true)) accessKeyId = caster.toString(arguments.get("accesskeyid", null), null);
		if (Util.isEmpty(accessKeyId, true)) accessKeyId = caster.toString(arguments.get("accesskey", null), null);
		if (Util.isEmpty(accessKeyId, true)) accessKeyId = caster.toString(arguments.get("awsAccessKeyId", null), null);

		// secretKey
		if (Util.isEmpty(secretAccessKey, true)) secretAccessKey = caster.toString(arguments.get("secretkey", null), null);
		if (Util.isEmpty(secretAccessKey, true)) secretAccessKey = caster.toString(arguments.get("awssecretkey", null), null);
		if (Util.isEmpty(secretAccessKey, true)) secretAccessKey = caster.toString(arguments.get("awsSecretKey", null), null);

		// region
		if (Util.isEmpty(region, true)) region = caster.toString(arguments.get("region", null), null);
		if (Util.isEmpty(region, true)) region = caster.toString(arguments.get("location", null), null);

		// host
		if (Util.isEmpty(host, true)) host = caster.toString(arguments.get("host", null), null);
		if (Util.isEmpty(host, true)) host = caster.toString(arguments.get("server", null), null);
		if (Util.isEmpty(host, true)) host = caster.toString(arguments.get("endpoint", null), null);

		liveTimeout = caster.toLongValue(arguments.get("liveTimeout", null), 3600000L);

		String logName = caster.toString(arguments.get("log", null), "application");
		if (!Util.isEmpty(logName, true) && config != null) {
			logName = logName.trim();
			this.log = config.getLog(logName);
		}

		if (log != null) {
			log.debug("dynamodb", "configuration: host:" + host + ";");
		}

		// Ensure table exists
		ensureTableExists();

	}

	private DynamoDbClient getClient() {
		// TODO may keep a local copy?
		return AmazonDynamoDBClient.get(accessKeyId, secretAccessKey, host, region, liveTimeout, log);
	}

	@Override
	public CacheEntry getCacheEntry(String key) throws IOException {
		try {
			GetItemRequest getRequest = GetItemRequest.builder().tableName(tableName).key(Map.of("cacheKey", AttributeValue.builder().s(key).build()))
					// No projection - fetch everything since CacheEntry needs all metadata
					.build();

			GetItemResponse response = getClient().getItem(getRequest);

			if (!valid(response)) {
				throw CFMLEngineFactory.getInstance().getExceptionUtil().createApplicationException("key [" + key + "] does not exist in table [" + tableName + "]");
			}

			return new DynamoDBCacheEntry(key, response.item(), log);

		}
		catch (Exception e) {
			throw handleException(e);
		}
	}

	@Override
	public CacheEntry getCacheEntry(String key, CacheEntry defaultValue) {
		try {
			GetItemRequest getRequest = GetItemRequest.builder().tableName(tableName).key(Map.of("cacheKey", AttributeValue.builder().s(key).build()))
					// No projection - fetch everything since CacheEntry needs all metadata
					.build();

			GetItemResponse response = getClient().getItem(getRequest);

			if (!valid(response)) {
				return defaultValue;
			}

			// Pass the entire item map, not just the value
			return new DynamoDBCacheEntry(key, response.item(), log);

		}
		catch (Exception e) {
			return defaultValue;
		}
	}

	@Override
	public void put(String key, Object value, Long idleTime, Long until) throws IOException {
		try {
			long nowMillis = System.currentTimeMillis();

			// Build update expressions
			StringBuilder updateExpr = new StringBuilder("SET ");
			updateExpr.append("#value = :value, ");
			updateExpr.append("#updatedTime = :now, ");
			updateExpr.append("#createdTime = if_not_exists(#createdTime, :now)"); // Only set if new

			Map<String, String> attrNames = new HashMap<>();
			attrNames.put("#value", "value"); // "value" is a reserved word in DynamoDB
			attrNames.put("#updatedTime", "updatedTime");
			attrNames.put("#createdTime", "createdTime");

			Map<String, AttributeValue> attrValues = new HashMap<>();
			attrValues.put(":value", Coder.toAttributeValue(value));
			attrValues.put(":now", AttributeValue.builder().n(String.valueOf(nowMillis)).build());

			// TTL
			Long expirationTime = calculateExpiration(nowMillis, idleTime, until);
			if (expirationTime != null) {
				updateExpr.append(", #ttl = :ttl");
				attrNames.put("#ttl", "ttl"); // "ttl" is a reserved word
				attrValues.put(":ttl", AttributeValue.builder().n(String.valueOf(expirationTime)).build());
			}

			// idle
			if (idleTime != null && idleTime > 0) {
				updateExpr.append(", #idle = :idle");
				attrNames.put("#idle", "idle");
				attrValues.put(":idle", AttributeValue.builder().n(String.valueOf(idleTime)).build());
			}
			// until
			if (until != null && until > 0) {
				updateExpr.append(", #until = :until");
				attrNames.put("#until", "until");
				attrValues.put(":until", AttributeValue.builder().n(String.valueOf(until)).build());
			}

			UpdateItemRequest updateRequest = UpdateItemRequest.builder().tableName(tableName).key(Map.of("cacheKey", AttributeValue.builder().s(key).build()))
					.updateExpression(updateExpr.toString()).expressionAttributeNames(attrNames).expressionAttributeValues(attrValues).build();

			getClient().updateItem(updateRequest);

		}
		catch (Exception e) {
			throw handleException(e);
		}
	}

	@Override
	public boolean contains(String key) throws IOException {
		try {
			GetItemRequest getRequest = GetItemRequest.builder().tableName(tableName).key(Map.of("cacheKey", AttributeValue.builder().s(key).build()))
					.projectionExpression("cacheKey, #ttl") // Fetch key and TTL
					.expressionAttributeNames(TTL_ATTR_NAMES) // ttl is a reserved word
					.build();

			GetItemResponse response = getClient().getItem(getRequest);

			return valid(response);
		}
		catch (Exception e) {
			throw handleException(e);
		}
	}

	@Override
	public boolean remove(String key) throws IOException {
		try {
			// Create the delete request
			DeleteItemRequest deleteRequest = DeleteItemRequest.builder().tableName(tableName).key(Map.of("cacheKey", AttributeValue.builder().s(key).build()))
					.returnValues(ReturnValue.ALL_OLD) // Returns old item if it existed
					.build();

			DeleteItemResponse response = getClient().deleteItem(deleteRequest);

			// Check if item existed AND was not expired
			Map<String, AttributeValue> oldItem = response.attributes();
			if (oldItem == null || oldItem.isEmpty()) {
				return false; // Item didn't exist
			}

			// Item existed, but was it valid (not expired)?
			return valid(oldItem);

		}
		catch (Exception e) {
			throw handleException(e);
		}
	}

	@Override
	public int remove(CacheKeyFilter filter) throws IOException {
		for (String k: keys(filter)) {
			remove(k);
		}
		return 0;
	}

	@Override
	public int remove(CacheEntryFilter filter) throws IOException {
		for (String k: keys(filter)) {
			remove(k);
		}
		return 0;
	}

	@Override
	public List<String> keys() throws IOException {
		return keys((CacheKeyFilter) null);
	}

	@Override
	public List<String> keys(CacheKeyFilter filter) throws IOException {
		try {
			List<String> result = new ArrayList<>();
			Map<String, AttributeValue> lastEvaluatedKey = null;

			do {
				// Build scan request
				ScanRequest.Builder scanBuilder = ScanRequest.builder().tableName(tableName);

				// Handle pagination
				if (lastEvaluatedKey != null) {
					scanBuilder.exclusiveStartKey(lastEvaluatedKey);
				}

				ScanRequest scanRequest = scanBuilder.build();
				ScanResponse response = getClient().scan(scanRequest);

				// Process items
				for (Map<String, AttributeValue> item: response.items()) {

					// Skip invalid/expired items
					if (!valid(item)) {
						continue;
					}
					// Get the key
					AttributeValue keyAttr = item.get("cacheKey");
					if (keyAttr == null || keyAttr.s() == null) {
						continue;
					}
					String key = keyAttr.s();

					// Apply filter if provided
					if (filter != null && !filter.accept(key)) {
						continue;
					}

					result.add(key);
				}

				// Get pagination token for next iteration
				lastEvaluatedKey = response.lastEvaluatedKey();

			}
			while (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty());

			return result;

		}
		catch (Exception e) {
			throw handleException(e);
		}
	}

	@Override
	public List<String> keys(CacheEntryFilter filter) throws IOException {
		List<CacheEntry> entries = entries(filter);
		List<String> result = new ArrayList<>(entries.size());

		for (CacheEntry e: entries) {
			result.add(e.getKey());
		}
		return result;
	}

	@Override
	public List<Object> values() throws IOException {
		return values((CacheKeyFilter) null);
	}

	@Override
	public List<Object> values(CacheKeyFilter filter) throws IOException {
		List<CacheEntry> entries = entries(filter);
		List<Object> result = new ArrayList<>(entries.size());
		for (CacheEntry e: entries) {
			result.add(e.getValue());
		}
		return result;
	}

	@Override
	public List<Object> values(CacheEntryFilter filter) throws IOException {
		List<CacheEntry> entries = entries(filter);
		List<Object> result = new ArrayList<>(entries.size());

		for (CacheEntry e: entries) {
			result.add(e.getValue());
		}
		return result;
	}

	@Override
	public List<CacheEntry> entries() throws IOException {
		return entries((CacheKeyFilter) null);
	}

	@Override
	public List<CacheEntry> entries(CacheKeyFilter filter) throws IOException {
		try {
			List<CacheEntry> result = new ArrayList<>();
			Map<String, AttributeValue> lastEvaluatedKey = null;

			do {
				// Build scan request
				ScanRequest.Builder scanBuilder = ScanRequest.builder().tableName(tableName);

				// Handle pagination
				if (lastEvaluatedKey != null) {
					scanBuilder.exclusiveStartKey(lastEvaluatedKey);
				}

				ScanRequest scanRequest = scanBuilder.build();
				ScanResponse response = getClient().scan(scanRequest);

				// Process items
				for (Map<String, AttributeValue> item: response.items()) {
					// Skip invalid/expired items
					if (!valid(item)) {
						continue;
					}

					// Get the key
					AttributeValue keyAttr = item.get("cacheKey");
					if (keyAttr == null || keyAttr.s() == null) {
						continue;
					}
					String key = keyAttr.s();

					// Apply filter if provided
					if (filter != null && !filter.accept(key)) {
						continue;
					}

					result.add(new DynamoDBCacheEntry(key, item, log));
				}

				// Get pagination token for next iteration
				lastEvaluatedKey = response.lastEvaluatedKey();

			}
			while (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty());

			return result;

		}
		catch (Exception e) {
			throw handleException(e);
		}
	}

	@Override
	public List<CacheEntry> entries(CacheEntryFilter filter) throws IOException {
		try {
			List<CacheEntry> result = new ArrayList<>();
			Map<String, AttributeValue> lastEvaluatedKey = null;

			do {
				// Build scan request
				ScanRequest.Builder scanBuilder = ScanRequest.builder().tableName(tableName);

				// Handle pagination
				if (lastEvaluatedKey != null) {
					scanBuilder.exclusiveStartKey(lastEvaluatedKey);
				}

				ScanRequest scanRequest = scanBuilder.build();
				ScanResponse response = getClient().scan(scanRequest);

				// Process items
				for (Map<String, AttributeValue> item: response.items()) {
					// Skip invalid/expired items
					if (!valid(item)) {
						continue;
					}

					// Get the key
					AttributeValue keyAttr = item.get("cacheKey");
					if (keyAttr == null || keyAttr.s() == null) {
						continue;
					}
					String key = keyAttr.s();

					// Create CacheEntry for filtering
					DynamoDBCacheEntry entry = new DynamoDBCacheEntry(key, item, log);

					// Apply filter if provided
					if (filter == null || filter.accept(entry)) {
						result.add(entry);
					}
				}

				// Get pagination token for next iteration
				lastEvaluatedKey = response.lastEvaluatedKey();

			}
			while (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty());

			return result;

		}
		catch (Exception e) {
			throw handleException(e);
		}
	}

	@Override
	public long hitCount() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long missCount() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Struct getCustomInfo() throws IOException {
		try {
			CFMLEngine eng = CFMLEngineFactory.getInstance();
			Struct data = eng.getCreationUtil().createStruct(Struct.TYPE_LINKED);

			// Basic configuration
			data.setEL("table", tableName);
			data.setEL("region", region);
			data.setEL("liveTimeout", liveTimeout);
			if (host != null) {
				data.setEL("host", host);
			}

			// Get table metadata from DynamoDB
			DynamoDbClient client = getClient();

			try {
				DescribeTableRequest describeRequest = DescribeTableRequest.builder().tableName(tableName).build();

				DescribeTableResponse describeResponse = client.describeTable(describeRequest);
				TableDescription table = describeResponse.table();

				// Table statistics
				data.setEL("itemCount", table.itemCount());
				data.setEL("tableSizeBytes", table.tableSizeBytes());
				data.setEL("tableStatus", table.tableStatusAsString());
				data.setEL("creationDateTime", eng.getCreationUtil().createDate(table.creationDateTime().toEpochMilli()));

				// Key schema
				Struct keySchema = eng.getCreationUtil().createStruct();
				for (KeySchemaElement key: table.keySchema()) {
					keySchema.setEL(key.attributeName(), key.keyTypeAsString());
				}
				data.setEL("keySchema", keySchema);

				// Billing mode
				if (table.billingModeSummary() != null) {
					data.setEL("billingMode", table.billingModeSummary().billingModeAsString());
				}

				// Provisioned throughput (if applicable)
				if (table.provisionedThroughput() != null) {
					Struct throughput = eng.getCreationUtil().createStruct();
					throughput.setEL("readCapacityUnits", table.provisionedThroughput().readCapacityUnits());
					throughput.setEL("writeCapacityUnits", table.provisionedThroughput().writeCapacityUnits());
					data.setEL("provisionedThroughput", throughput);
				}

				// TTL configuration
				try {
					DescribeTimeToLiveRequest ttlRequest = DescribeTimeToLiveRequest.builder().tableName(tableName).build();

					DescribeTimeToLiveResponse ttlResponse = client.describeTimeToLive(ttlRequest);
					if (ttlResponse.timeToLiveDescription() != null) {
						Struct ttlInfo = eng.getCreationUtil().createStruct();
						ttlInfo.setEL("status", ttlResponse.timeToLiveDescription().timeToLiveStatusAsString());
						if (ttlResponse.timeToLiveDescription().attributeName() != null) {
							ttlInfo.setEL("attributeName", ttlResponse.timeToLiveDescription().attributeName());
						}
						data.setEL("ttl", ttlInfo);
					}
				}
				catch (Exception e) {
					// TTL info is optional, continue if it fails
					if (log != null) {
						log.debug("dynamodb-cache", "Could not retrieve TTL info: " + e.getMessage());
					}
				}

				// Global Secondary Indexes (if any)
				if (table.hasGlobalSecondaryIndexes()) {
					Array indexes = eng.getCreationUtil().createArray();
					for (GlobalSecondaryIndexDescription gsi: table.globalSecondaryIndexes()) {
						Struct indexInfo = eng.getCreationUtil().createStruct();
						indexInfo.setEL("name", gsi.indexName());
						indexInfo.setEL("status", gsi.indexStatusAsString());
						indexInfo.setEL("itemCount", gsi.itemCount());
						indexInfo.setEL("sizeBytes", gsi.indexSizeBytes());
						indexes.appendEL(indexInfo);
					}
					data.setEL("globalSecondaryIndexes", indexes);
				}

			}
			catch (Exception e) {
				throw CFMLEngineFactory.getInstance().getExceptionUtil().toIOException(e);
			}

			return data;

		}
		catch (Exception e) {
			throw handleException(e);
		}
	}

	@Override
	public CacheEntry getQuiet(String key, CacheEntry defaultValue) {
		return getCacheEntry(key, defaultValue);
	}

	@Override
	public int clear() throws IOException {
		// Remove all items by passing null filter (accepts everything)
		return remove((CacheKeyFilter) null);
	}

	private void ensureTableExists() throws IOException {
		try {
			DynamoDbClient client = getClient();

			// Check if table exists
			try {
				DescribeTableRequest describeRequest = DescribeTableRequest.builder().tableName(tableName).build();
				client.describeTable(describeRequest);

				if (log != null) {
					log.debug("dynamodb-cache", "Table '" + tableName + "' already exists");
				}
				return; // Table exists, we're done

			}
			catch (ResourceNotFoundException e) {
				// Table doesn't exist, create it
				if (log != null) {
					log.info("dynamodb-cache", "Table '" + tableName + "' does not exist, creating...");
				}
			}

			// Create table
			CreateTableRequest createRequest = CreateTableRequest.builder().tableName(tableName)
					.keySchema(KeySchemaElement.builder().attributeName("cacheKey").keyType(KeyType.HASH).build())
					.attributeDefinitions(AttributeDefinition.builder().attributeName("cacheKey").attributeType(ScalarAttributeType.S).build())
					.billingMode(BillingMode.PAY_PER_REQUEST) // On-demand billing, no capacity planning needed
					.build();

			client.createTable(createRequest);

			if (log != null) {
				log.info("dynamodb", "Table '" + tableName + "' created successfully");
			}

			// Wait for table to be active
			waitForTableActive(client, tableName);

			// Enable TTL
			enableTTL(client);

		}
		catch (Exception e) {
			throw handleException(e);
		}
	}

	private void waitForTableActive(DynamoDbClient client, String tableName) throws IOException {
		try {
			int maxAttempts = 30;
			int attempt = 0;

			while (attempt < maxAttempts) {
				try {
					DescribeTableRequest describeRequest = DescribeTableRequest.builder().tableName(tableName).build();

					DescribeTableResponse response = client.describeTable(describeRequest);

					if (response.table().tableStatus() == TableStatus.ACTIVE) {
						if (log != null) {
							log.debug("dynamodb-cache", "Table '" + tableName + "' is now active");
						}
						return;
					}

					Thread.sleep(1000); // Wait 1 second before next check
					attempt++;

				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new IOException("Interrupted while waiting for table to become active", e);
				}
			}

			throw new IOException("Timeout waiting for table '" + tableName + "' to become active");

		}
		catch (Exception e) {
			throw handleException(e);
		}
	}

	private void enableTTL(DynamoDbClient client) {
		try {
			UpdateTimeToLiveRequest ttlRequest = UpdateTimeToLiveRequest.builder().tableName(tableName)
					.timeToLiveSpecification(TimeToLiveSpecification.builder().enabled(true).attributeName("ttl").build()).build();

			client.updateTimeToLive(ttlRequest);

			if (log != null) {
				log.info("dynamodb", "TTL enabled on table '" + tableName + "'");
			}

		}
		catch (Exception e) {
			// TTL enablement is best-effort, don't fail if it doesn't work
			if (log != null) {
				log.warn("dynamodb", "Could not enable TTL: " + e.getMessage());
			}
		}
	}

	//////////////////// helper methods /////////////////////

	private Long calculateExpiration(long nowMillis, Long idleTime, Long until) {
		// TODO check if that is correct

		// 'until' is absolute expiration time (epoch milliseconds)
		if (until != null && until > 0) {
			return (nowMillis + until) / 1000; // Convert to seconds for DynamoDB TTL
		}

		// 'idleTime' is relative time in milliseconds
		if (idleTime != null && idleTime > 0) {
			return (nowMillis + idleTime) / 1000;
		}

		return null; // No expiration
	}

	private IOException handleException(Exception e) {
		return CFMLEngineFactory.getInstance().getExceptionUtil().toIOException(e);
	}

	private final boolean valid(GetItemResponse response) throws PageException {
		// Only check expiration if item exists
		if (!response.hasItem()) {
			return false; // No item means not expired (just doesn't exist)
		}

		Map<String, AttributeValue> item = response.item();
		if (item.containsKey("ttl")) {
			return (System.currentTimeMillis() / 1000) < (eng.getCastUtil().toLongValue(item.get("ttl").n()));
		}
		return true;
	}

	private boolean valid(Map<String, AttributeValue> item) throws PageException {
		if (item == null || item.isEmpty()) {
			return false;
		}

		if (item.containsKey("ttl")) {
			long ttlSeconds = eng.getCastUtil().toLongValue(item.get("ttl").n());
			return (System.currentTimeMillis() / 1000) < ttlSeconds;
		}
		return true;
	}

}
