package org.lucee.extension.aws.dynamodb;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Date;

import lucee.commons.io.cache.Cache;
import lucee.commons.io.cache.CacheEntry;
import lucee.commons.io.cache.exp.CacheException;

/**
 * this class handles all action that oare independent of the cache type used
 */
public abstract class CacheSupport implements Cache {

	public static final Charset UTF8 = Charset.forName("UTF-8");

	@Override
	public Object getValue(String key) throws IOException {
		return getCacheEntry(key).getValue();
	}

	@Override
	public Object getValue(String key, Object defaultValue) {
		CacheEntry entry = getCacheEntry(key, null);
		if (entry == null) return defaultValue;
		return entry.getValue();
	}

	protected static boolean valid(CacheEntry entry) {
		if (entry == null) return false;
		long now = System.currentTimeMillis();
		if (entry.liveTimeSpan() > 0 && entry.liveTimeSpan() + getTime(entry.lastModified()) < now) {
			return false;
		}
		if (entry.idleTimeSpan() > 0 && entry.idleTimeSpan() + getTime(entry.lastHit()) < now) {
			return false;
		}
		return true;
	}

	private static long getTime(Date date) {
		return date == null ? 0 : date.getTime();
	}

	public CacheEntry getQuiet(String key) throws IOException {
		CacheEntry entry = getQuiet(key, null);
		if (entry == null) throw new CacheException("there is no valid cache entry with key [" + key + "]");
		return entry;
	}

	public abstract CacheEntry getQuiet(String key, CacheEntry defaultValue);

	// CachePro interface @Override
	public abstract int clear() throws IOException;
}