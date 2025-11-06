package org.lucee.extension.aws.dynamodb.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.bson.BsonDocument;
import org.bson.BsonValue;

import lucee.loader.engine.CFMLEngine;
import lucee.loader.engine.CFMLEngineFactory;
import lucee.loader.util.Util;
import lucee.runtime.exp.PageException;
import lucee.runtime.type.Array;
import lucee.runtime.type.Collection.Key;
import lucee.runtime.type.Struct;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class Coder {

	private static final byte GZIP0 = (byte) 0x1f;
	private static final byte GZIP1 = (byte) 0x8b;

	private static DecimalFormat ff = (DecimalFormat) DecimalFormat.getInstance(Locale.US);
	private static CFMLEngine eng;
	static {
		ff.applyLocalizedPattern("#.#######");
	}

	private static byte[] OBJECT_STREAM_HEADER = new byte[] { -84, -19, 0, 5 };

	public static final Charset UTF8 = Charset.forName("UTF-8");

	public static byte[] toKey(String key) {
		return key.trim().toLowerCase().getBytes(UTF8);
	}

	public static byte[] toBytes(String val) {
		return val.getBytes(UTF8);
	}

	public static byte[][] toBytesArrays(String[] values) {
		byte[][] results = new byte[values.length][];
		for (int i = 0; i < results.length; i++) {
			results[i] = Coder.toBytes(values[i]);
		}
		return results;
	}

	public static String toKey(byte[] bkey) {
		return new String(bkey, UTF8);
	}

	public static String toString(byte[] bkey) {
		return new String(bkey, UTF8);
	}

	public static String toStringKey(String key) {
		return key.trim().toLowerCase();
	}

	public static byte[][] toKeys(String[] keys) {
		byte[][] arr = new byte[keys == null ? 0 : keys.length][];
		for (int i = 0; i < keys.length; i++) {
			arr[i] = keys[i].trim().toLowerCase().getBytes(UTF8);
		}
		return arr;
	}

	public static ValueSizePair evaluate(AttributeValue valueAttr) throws IOException, PageException {
		return evaluate(Thread.currentThread().getContextClassLoader(), valueAttr);
	}

	public static ValueSizePair evaluate(ClassLoader cl, AttributeValue valueAttr) throws IOException, PageException {

		// null
		if (valueAttr.nul() != null && valueAttr.nul()) {
			return ValueSizePair.NULL;
		}
		// String
		if (valueAttr.s() != null) {
			return new ValueSizePair(valueAttr.s(), valueAttr.s().length());
		}
		// Boolean
		if (valueAttr.bool() != null) {
			return valueAttr.bool() ? ValueSizePair.TRUE : ValueSizePair.FALSE;
		}

		if (eng == null) {
			// this fails when executed outside a Lucee engine
			try {
				eng = CFMLEngineFactory.getInstance();
			}
			catch (Exception e) {
			}
		}

		// Number
		if (valueAttr.n() != null) {
			String str = valueAttr.n();
			return new ValueSizePair(eng.getCastUtil().toBigDecimal(str), str.length());

		}

		// Map
		if (valueAttr.hasM()) {
			int size = 0;
			Struct sct = eng.getCreationUtil().createStruct(Struct.TYPE_LINKED);
			ValueSizePair vsp;
			for (Entry<String, AttributeValue> e: valueAttr.m().entrySet()) {
				vsp = evaluate(cl, e.getValue());
				sct.set(e.getKey(), vsp.value);
				size += vsp.size;

			}
			return new ValueSizePair(sct, size);
		}

		// Collection
		if (valueAttr.hasL()) {
			int size = 0;
			Array arr = eng.getCreationUtil().createArray();
			ValueSizePair vsp;
			for (AttributeValue av: valueAttr.l()) {
				vsp = evaluate(cl, av);
				arr.append(vsp.value);
				size += vsp.size;
			}
			return new ValueSizePair(arr, size);
		}

		// binary
		SdkBytes sdkBytes = valueAttr.b();
		if (sdkBytes == null) throw eng.getExceptionUtil().createApplicationException("could not extract a value from the given AttributeValue instance");
		byte[] data = sdkBytes.asByteArray();

		if (data == null) return null;
		if (isGzip(data)) {
			return new ValueSizePair(decompress(cl, data), data.length);
		}

		if (!isObjectStream(data)) {
			BsonDocument doc = BSON.toBsonDocument(data, null);
			if (doc != null) {
				if (doc.getFirstKey().equals(BSON.IK_STORAGEVALUE_KEY)) {
					Iterator<Entry<String, BsonValue>> it = doc.entrySet().iterator();
					Entry<String, BsonValue> first = it.next();
					BsonValue v = first.getValue();
					if (v.isInt64() && first.getKey().equals(BSON.IK_STORAGEVALUE_KEY)) {
						return new ValueSizePair(BSON.toIKStorageValue(v.asInt64().longValue(), it, CFMLEngineFactory.getInstance()), data.length);
					}
				}
				return new ValueSizePair(BSON.toStruct(doc, CFMLEngineFactory.getInstance()), data.length);
			}
			return new ValueSizePair(toString(data), data.length);
		}

		ByteArrayInputStream bais = new ByteArrayInputStream(data);
		ObjectInputStream ois = null;
		try {
			ois = new ObjectInputStreamImpl(cl, bais);
			return new ValueSizePair(ois.readObject(), data.length);
		}
		catch (ClassNotFoundException cnfe) {
			String className = cnfe.getMessage();
			if (!Util.isEmpty(className, true)) {
				Class<?> clazz = CFMLEngineFactory.getInstance().getClassUtil().loadClass(className.trim());
				bais = new ByteArrayInputStream(data);
				ois = new ObjectInputStreamImpl(clazz.getClassLoader(), bais);
				try {
					return new ValueSizePair(ois.readObject(), data.length);
				}
				catch (ClassNotFoundException e) {
					throw CFMLEngineFactory.getInstance().getExceptionUtil().toIOException(e);
				}
			}
			try {
				return new ValueSizePair(toString(data), data.length);
			}
			catch (Exception ee) {
				throw CFMLEngineFactory.getInstance().getExceptionUtil().toIOException(cnfe);
			}
		}
		// happens when the object is not ObjectOutputstream serialized
		catch (Exception e) {
			try {
				return new ValueSizePair(toString(data), data.length);
			}
			catch (Exception ee) {
				throw CFMLEngineFactory.getInstance().getExceptionUtil().toIOException(e);
			}
		}
		finally {
			Util.closeEL(ois);
		}
	}

	private static boolean isObjectStream(byte[] data) {
		if (data == null || data.length < OBJECT_STREAM_HEADER.length) return false;
		for (int i = 0; i < OBJECT_STREAM_HEADER.length; i++) {
			if (data[i] != OBJECT_STREAM_HEADER[i]) return false;
		}
		return true;
	}

	public static AttributeValue toAttributeValue(Object value) throws IOException, PageException {
		// null
		if (value == null) {
			return AttributeValue.builder().nul(true).build();
		}
		// String
		if (value instanceof CharSequence) {
			return AttributeValue.builder().s(value.toString()).build();
		}
		// Boolean
		if (value instanceof Boolean) {
			return AttributeValue.builder().bool((Boolean) value).build();
		}

		if (eng == null) {
			// this fails when executed outside a Lucee engine
			try {
				eng = CFMLEngineFactory.getInstance();
			}
			catch (Exception e) {
			}
		}
		// Number
		if (value instanceof Number) {
			return AttributeValue.builder().n(eng.getCastUtil().toString(value)).build();
		}

		// Struct
		if (value instanceof Struct) {
			Map<String, AttributeValue> trg = new HashMap<>();
			Iterator<Entry<Key, Object>> it = ((Struct) value).entryIterator();
			Entry<Key, Object> e;
			while (it.hasNext()) {
				e = it.next();
				trg.put(e.getKey().getString(), toAttributeValue(e.getValue()));
			}
			return AttributeValue.builder().m(trg).build();
		}

		// Map
		if (value instanceof Map) {
			Map<String, AttributeValue> trg = new HashMap<>();
			for (Entry e: ((Set<Map.Entry>) ((Map) value).entrySet())) {
				trg.put(eng.getCastUtil().toString(e.getKey()), toAttributeValue(e.getValue()));
			}
			return AttributeValue.builder().m(trg).build();
		}

		// Array
		if (value instanceof Array) {
			List<AttributeValue> trg = new ArrayList<>();
			Iterator<?> it = ((Array) value).getIterator();
			while (it.hasNext()) {
				trg.add(toAttributeValue(it.next()));
			}
			return AttributeValue.builder().l(trg).build();
		}

		// Collection
		if (value instanceof Collection) {
			List<AttributeValue> trg = new ArrayList<>();
			for (Object v: (Collection) value) {
				trg.add(toAttributeValue(v));
			}
			return AttributeValue.builder().l(trg).build();
		}

		// Binary
		if (eng.getDecisionUtil().isBinary(value)) {
			return AttributeValue.builder().b(software.amazon.awssdk.core.SdkBytes.fromByteArray(eng.getCastUtil().toBinary(value))).build();
		}

		// if we cannot do it directly, we create a bson
		BsonDocument doc = BSON.toBsonDocument(value, false, null);
		if (doc != null) {
			return AttributeValue.builder().b(software.amazon.awssdk.core.SdkBytes.fromByteArray(BSON.toBytes(doc))).build();
		}

		// if that fails we try to ObjectOutputStream
		return AttributeValue.builder().b(software.amazon.awssdk.core.SdkBytes.fromByteArray(compress(value))).build();
	}

	public static byte[] compress(Object val) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(new GZIPOutputStream(baos));
		oos.writeObject(val);
		oos.close();
		return baos.toByteArray();
	}

	public static Object decompress(ClassLoader cl, byte[] bytes) throws IOException {
		ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
		ObjectInputStreamImpl objectIn = new ObjectInputStreamImpl(cl, new GZIPInputStream(bais));
		try {
			Object val = objectIn.readObject();
			objectIn.close();
			return val;
		}
		catch (ClassNotFoundException cnfe) {
			String className = cnfe.getMessage();
			if (!Util.isEmpty(className, true)) {
				Class<?> clazz = CFMLEngineFactory.getInstance().getClassUtil().loadClass(className.trim());
				bais = new ByteArrayInputStream(bytes);
				objectIn = new ObjectInputStreamImpl(clazz.getClassLoader(), bais);
				try {
					return objectIn.readObject();
				}
				catch (ClassNotFoundException e) {
					throw CFMLEngineFactory.getInstance().getExceptionUtil().toIOException(e);
				}
			}

			throw CFMLEngineFactory.getInstance().getExceptionUtil().toIOException(cnfe);
		}
	}

	public static boolean isGzip(byte[] barr) throws IOException {
		return barr != null && barr.length > 1 && barr[0] == GZIP0 && barr[1] == GZIP1;
	}

	private static String toString(float f) {
		long l = (long) f;
		if (l == f) return Long.toString(l, 10);

		if (f > l && (f - l) < 0.000000000001) return Long.toString(l, 10);
		if (l > f && (l - f) < 0.000000000001) return Long.toString(l, 10);

		return ff.format(f);
	}

	public static class ValueSizePair {
		public static final ValueSizePair NULL = new ValueSizePair(null, 0);
		public static final ValueSizePair TRUE = new ValueSizePair(Boolean.TRUE, 4);
		public static final ValueSizePair FALSE = new ValueSizePair(Boolean.FALSE, 5);
		public final Object value;
		public final long size;

		public ValueSizePair(Object value, long size) {
			this.value = value;
			this.size = size;
		}

	}

}
