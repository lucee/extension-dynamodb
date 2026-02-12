package org.lucee.extension.aws.dynamodb.util;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.lucee.extension.aws.dynamodb.DynamoDBCache;

import lucee.commons.io.cache.Cache;
import lucee.commons.io.cache.CacheEntry;
import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.runtime.PageContext;
import lucee.runtime.exp.PageException;
import lucee.runtime.type.Struct;
import lucee.runtime.type.dt.TimeSpan;

public class CommonUtil {
	private static final Map<String, String> tokens = new ConcurrentHashMap<>();
	private static final Class[] GET_CACHE_PARAMS = new Class[] { PageContext.class, String.class, int.class };
	public static final int CACHE_TYPE_OBJECT = 1;
	private static Method getCache;

	public static String createToken(String prefix, String name) {
		String str = prefix + ":" + name;
		String lock = tokens.putIfAbsent(str, str);
		if (lock == null) {
			lock = str;
		}
		return lock;
	}

	public static Struct getInfo(CacheEntry ce) {
		Struct info = CFMLEngineFactory.getInstance().getCreationUtil().createStruct();
		info.setEL("key", ce.getKey());
		info.setEL("created", ce.created());
		info.setEL("last_hit", ce.lastHit());
		info.setEL("last_modified", ce.lastModified());

		info.setEL("hit_count", new Double(ce.hitCount()));
		info.setEL("size", new Double(ce.size()));

		info.setEL("idle_time_span", toTimespan(ce.idleTimeSpan()));
		info.setEL("live_time_span", toTimespan(ce.liveTimeSpan()));

		return info;
	}

	public static Object toTimespan(long timespan) {
		if (timespan == 0) return "";

		TimeSpan ts = CFMLEngineFactory.getInstance().getCastUtil().toTimespan(timespan);

		if (ts == null) return "";
		return ts;
	}

	public static DynamoDBCache getDynamoDBCache(PageContext pc, CFMLEngine eng, String cacheName) throws PageException, IOException {

		Cache cache = getCache(pc, cacheName);
		if (cache instanceof DynamoDBCache) {

			return ((DynamoDBCache) cache);
		}

		throw eng.getExceptionUtil().createApplicationException("cache [" + cacheName + "; class:" + cache.getClass().getName() + "] is not a dynamodb cache");

	}

	private static Cache getCache(PageContext pc, String cacheName) throws PageException {
		CFMLEngine eng = CFMLEngineFactory.getInstance();

		try {
			ClassLoader cl = pc.getClass().getClassLoader();
			if (getCache == null || !getCache.getDeclaringClass().getClassLoader().equals(cl)) {
				Class<?> cacheUtil = eng.getClassUtil().loadClass(cl, "lucee.runtime.cache.CacheUtil");
				getCache = cacheUtil.getMethod("getCache", GET_CACHE_PARAMS);
			}
			return (Cache) getCache.invoke(null, new Object[] { pc, cacheName, CACHE_TYPE_OBJECT });
		}
		catch (Exception e) {
			throw eng.getCastUtil().toPageException(e);
		}
	}
}
