package org.lucee.extension.aws.dynamodb;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

import org.lucee.extension.aws.dynamodb.util.Coder;
import org.lucee.extension.aws.dynamodb.util.Coder.ValueSizePair;
import org.lucee.extension.aws.dynamodb.util.CommonUtil;

import lucee.commons.io.cache.CacheEntry;
import lucee.commons.io.log.Log;
import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.runtime.exp.PageException;
import lucee.runtime.type.Struct;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class DynamoDBCacheEntry implements CacheEntry {

	private String key;
	private Object value;
	private Date[] created;
	private Date[] lastModified;
	private long[] idle;
	private long[] until;
	private long size;
	private Map<String, AttributeValue> item;
	private Log log;

	public DynamoDBCacheEntry(String key, Map<String, AttributeValue> item, Log log) throws PageException, IOException {
		// Extract key
		// AttributeValue keyAttr = item.get("cacheKey");
		// this.key = keyAttr != null && keyAttr.s() != null ? keyAttr.s() : null;
		this.key = key;
		this.item = item;
		this.log = log;

		// Extract value
		AttributeValue valueAttr = item.get("value");
		if (valueAttr != null) {
			ValueSizePair vsp = Coder.evaluate(Thread.currentThread().getContextClassLoader(), valueAttr);
			this.value = vsp.value;
			this.size = vsp.size;
		}

	}

	@Override
	public Date lastHit() {
		// DynamoDB doesn't track access time by default
		// Would require additional attributes and update logic
		return null;
	}

	@Override
	public Date lastModified() {
		if (lastModified == null) {
			// createdTime is in milliseconds
			AttributeValue lastModifiedAttr = item.get("updatedTime");
			if (lastModifiedAttr != null && lastModifiedAttr.n() != null) {
				CFMLEngine eng = CFMLEngineFactory.getInstance();
				try {
					this.lastModified = new Date[] { eng.getCastUtil().toDate(eng.getCastUtil().toLong(lastModifiedAttr.n()), null) };
					return this.lastModified[0];
				}
				catch (Exception e) {
					if (log != null) {
						log.error("dynamodb", e);
					}
				}
			}
			this.lastModified = new Date[] { null };
			return null;
		}
		return lastModified[0];
	}

	@Override
	public Date created() {
		if (created == null) {
			// createdTime is in milliseconds
			AttributeValue createdAttr = item.get("createdTime");
			if (createdAttr != null && createdAttr.n() != null) {
				CFMLEngine eng = CFMLEngineFactory.getInstance();
				try {
					this.created = new Date[] { eng.getCastUtil().toDate(eng.getCastUtil().toLong(createdAttr.n()), null) };
				}
				catch (Exception e) {
					if (log != null) {
						log.error("dynamodb", e);
					}
					this.created = new Date[] { lastModified() };
				}
			}
			else {
				this.created = new Date[] { null };
				return null;
			}
		}
		return created[0];
	}

	@Override
	public int hitCount() {
		// TODO store hitcount in storage (optional feature for debugging)
		return 0;
	}

	@Override
	public String getKey() {
		return key;
	}

	@Override
	public Object getValue() {
		return value;
	}

	@Override
	public long size() {
		return size;
	}

	@Override
	public long liveTimeSpan() {
		if (until == null) {
			AttributeValue attr = item.get("until");
			if (attr != null && attr.n() != null) {
				CFMLEngine eng = CFMLEngineFactory.getInstance();
				try {
					this.until = new long[] { eng.getCastUtil().toLong(attr.n()) };
					return this.until[0];
				}
				catch (Exception e) {
					if (log != null) {
						log.error("dynamodb", e);
					}
				}
			}
			this.until = new long[] { 0 };
			return 0;
		}
		return until[0];
	}

	@Override
	public long idleTimeSpan() {
		if (idle == null) {
			AttributeValue attr = item.get("idle");
			if (attr != null && attr.n() != null) {
				CFMLEngine eng = CFMLEngineFactory.getInstance();
				try {
					this.idle = new long[] { eng.getCastUtil().toLong(attr.n()) };
					return this.idle[0];
				}
				catch (Exception e) {
					if (log != null) {
						log.error("dynamodb", e);
					}
				}
			}
			this.idle = new long[] { 0 };
			return 0;
		}
		return idle[0];
	}

	@Override
	public Struct getCustomInfo() {
		Struct data = CommonUtil.getInfo(this);

		for (Entry<String, AttributeValue> e: item.entrySet()) {
			if ("value".equals(e.getKey()) || "idle".equals(e.getKey()) || "until".equals(e.getKey()) || "createdTime".equals(e.getKey()) || "updatedTime".equals(e.getKey()))
				continue;

			try {
				data.put(e.getKey(), Coder.evaluate(e.getValue()));
			}
			catch (Exception ex) {
				if (log != null) {
					log.error("dynamodb", ex);
				}
			}
		}
		return data;
	}
}