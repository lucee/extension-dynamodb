package org.lucee.extension.aws.dynamodb.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import lucee.commons.io.cache.CacheEntry;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.runtime.type.Struct;
import lucee.runtime.type.dt.TimeSpan;

public class CommonUtil {
	private static final Map<String, String> tokens = new ConcurrentHashMap<>();

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
}
