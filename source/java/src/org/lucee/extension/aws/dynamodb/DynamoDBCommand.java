package org.lucee.extension.aws.dynamodb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.lucee.extension.aws.dynamodb.util.Coder;
import org.lucee.extension.aws.dynamodb.util.CommonUtil;

import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.runtime.PageContext;
import lucee.runtime.exp.PageException;
import lucee.runtime.ext.function.BIF;
import lucee.runtime.ext.function.Function;
import lucee.runtime.type.Collection.Key;
import lucee.runtime.type.Struct;
import lucee.runtime.util.Cast;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest.Builder;

public class DynamoDBCommand extends BIF implements Function {

	private static final long serialVersionUID = 4792638153025545116L;
	private static Key RETURN_VALUES;
	static {
		RETURN_VALUES = CFMLEngineFactory.getInstance().getCastUtil().toKey("ReturnValues");

	}

	@Override
	public Object invoke(PageContext pc, Object[] args) throws PageException {
		CFMLEngine eng = CFMLEngineFactory.getInstance();
		Cast cast = eng.getCastUtil();

		if (args.length < 1 || args.length > 3) {
			throw eng.getExceptionUtil().createFunctionException(pc, "DynamoDBCommand", 2, 3, args.length);
		}

		String action = cast.toString(args[0]);

		// if no cache name is provided it will pick the default cache
		String cacheName = args.length == 3 ? cast.toString(args[2]) : null;

		// Get the client from the existing cache connection

		try {
			DynamoDBCache cache = CommonUtil.getDynamoDBCache(pc, eng, cacheName);
			cache.getClient();
			// UPDATE
			if (action.equalsIgnoreCase("updateItem")) {
				Struct data = secondAsStruct(eng, pc, action, args);
				software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest.Builder builder = software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest.builder()
						.tableName(cache.getTableName()).key(toAttributeMap(data.get("key"))); // The PK to find the item

				if (data.containsKey("UpdateExpression")) builder.updateExpression(cast.toString(data.get("UpdateExpression")));

				if (data.containsKey("ExpressionAttributeValues")) builder.expressionAttributeValues(toAttributeMap(data.get("ExpressionAttributeValues")));

				if (data.containsKey("ExpressionAttributeNames")) builder.expressionAttributeNames(cast.toStruct(data.get("ExpressionAttributeNames")));

				if (data.containsKey(RETURN_VALUES)) builder.returnValues(cast.toString(data.get(RETURN_VALUES)));

				return fromAttributeMap(cache.getClient().updateItem(builder.build()).attributes());
			}
			// PUT
			else if (action.equalsIgnoreCase("putItem")) {
				Struct data = secondAsStruct(eng, pc, action, args);
				Builder builder = PutItemRequest.builder().tableName(cache.getTableName()).item(toAttributeMap(data));

				if (data.containsKey(RETURN_VALUES)) {
					builder.returnValues(cast.toString(data.get(RETURN_VALUES)));
				}

				return fromAttributeMap(cache.getClient().putItem(builder.build()).attributes());
			}

			// GET
			else if (action.equalsIgnoreCase("getItem")) {
				Struct keyData = secondAsStruct(eng, pc, action, args);

				GetItemRequest request = GetItemRequest.builder().tableName(cache.getTableName()).key(toAttributeMap(keyData)).build();

				return fromAttributeMap(cache.getClient().getItem(request).item());
			}

			// DELETE
			else if (action.equalsIgnoreCase("deleteItem")) {
				Struct keyData = secondAsStruct(eng, pc, action, args);

				DeleteItemRequest request = DeleteItemRequest.builder().tableName(cache.getTableName()).key(toAttributeMap(keyData))
						// Optional: return the item as it was before it was deleted
						.returnValues(keyData.containsKey(RETURN_VALUES) ? cast.toString(keyData.get(RETURN_VALUES)) : "NONE").build();

				return fromAttributeMap(cache.getClient().deleteItem(request).attributes());
			}

			// QUERY
			else if (action.equalsIgnoreCase("query")) {
				Struct data = secondAsStruct(eng, pc, action, args);

				software.amazon.awssdk.services.dynamodb.model.QueryRequest.Builder builder = software.amazon.awssdk.services.dynamodb.model.QueryRequest.builder()
						.tableName(cache.getTableName());

				if (data.containsKey("KeyConditionExpression")) {
					builder.keyConditionExpression(cast.toString(data.get("KeyConditionExpression")));
				}

				if (data.containsKey("ExpressionAttributeValues")) {
					builder.expressionAttributeValues(toAttributeMap(data.get("ExpressionAttributeValues")));
				}

				if (data.containsKey("FilterExpression")) {
					builder.filterExpression(cast.toString(data.get("FilterExpression")));
				}

				if (data.containsKey("IndexName")) {
					builder.indexName(cast.toString(data.get("IndexName")));
				}

				return fromAttributeList(cache.getClient().query(builder.build()).items());
			}

			// SCAN
			else if (action.equalsIgnoreCase("scan")) {
				Struct data = secondAsStruct(eng, pc, action, args);

				software.amazon.awssdk.services.dynamodb.model.ScanRequest.Builder builder = software.amazon.awssdk.services.dynamodb.model.ScanRequest.builder()
						.tableName(cache.getTableName());

				// 1. Filter Expression (Server-side filtering before data reaches Lucee)
				if (data.containsKey("FilterExpression")) {
					builder.filterExpression(cast.toString(data.get("FilterExpression")));
				}

				// 2. Expression Attribute Values (for the filter placeholders)
				if (data.containsKey("ExpressionAttributeValues")) {
					builder.expressionAttributeValues(toAttributeMap(data.get("ExpressionAttributeValues")));
				}

				// 3. Optional: Index Name (Scan a specific GSI instead of the base table)
				if (data.containsKey("IndexName")) {
					builder.indexName(cast.toString(data.get("IndexName")));
				}

				// 4. Limit (Max items to evaluate)
				if (data.containsKey("Limit")) {
					builder.limit(cast.toIntValue(data.get("Limit")));
				}

				return fromAttributeList(cache.getClient().scan(builder.build()).items());
			}
			else {
				throw eng.getExceptionUtil().createFunctionException(pc, "DynamoDBCommand", 1, "action", "action [" + action + "] is not supported yet.", null);
			}

		}
		catch (Exception e) {
			throw cast.toPageException(e);
		}

	}

	private static Struct secondAsStruct(CFMLEngine eng, PageContext pc, String action, Object[] args) throws PageException {
		if (args.length < 2) {
			throw eng.getExceptionUtil().createFunctionException(pc, "DynamoDBCommand", 2, "item", "for action [" + action + "] 2 arguments are required", null);
		}
		return eng.getCastUtil().toStruct(args[1]);
	}

	/**
	 * Helper to convert Lucee Structs to DynamoDB Attribute Maps using your Coder
	 */
	private Map<String, AttributeValue> toAttributeMap(Object input) throws IOException, PageException {
		if (!(input instanceof Struct)) return new HashMap<>();
		Struct sct = (Struct) input;
		Map<String, AttributeValue> map = new HashMap<>();

		Iterator<Key> it = sct.keyIterator();
		while (it.hasNext()) {
			Key k = it.next();
			map.put(k.getString(), Coder.toAttributeValue(sct.get(k, null)));
		}
		return map;
	}

	/**
	 * Helper to convert DynamoDB Response Maps back to Lucee Objects using your Coder
	 */
	private Object fromAttributeMap(Map<String, AttributeValue> item) throws IOException, PageException {
		if (item == null || item.isEmpty()) return null;
		Struct result = CFMLEngineFactory.getInstance().getCreationUtil().createStruct();

		for (Map.Entry<String, AttributeValue> entry: item.entrySet()) {
			result.setEL(entry.getKey(), Coder.evaluate(entry.getValue()).value);
		}
		return result;
	}

	private lucee.runtime.type.Array fromAttributeList(java.util.List<Map<String, AttributeValue>> items) throws IOException, PageException {
		lucee.runtime.type.Array arr = CFMLEngineFactory.getInstance().getCreationUtil().createArray();
		if (items != null) {
			for (Map<String, AttributeValue> item: items) {
				arr.append(fromAttributeMap(item));
			}
		}
		return arr;
	}

}