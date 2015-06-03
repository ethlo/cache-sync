package com.ethlo.cache.notification;

import java.util.Map.Entry;

import com.ethlo.keyvalue.range.RangeCache;
import com.google.common.collect.Range;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;

/**
 * 
 * @author Morten Haraldsen
 *
 * @param <K>
 * @param <V>
 */
public class SyncedRangeCache<K extends Comparable<K>,V> implements RangeCache<K,V>
{
	private final RangeCache<K, V> delegate;
	private ITopic<RangeCacheInvalidationMessage<K>> topic;

	public SyncedRangeCache(RangeCache<K, V> rangeCache, HazelcastInstance hazelcastInstance, final String cacheId)
	{
		this.topic = hazelcastInstance.getTopic("_invalidation_" + cacheId);
		topic.addMessageListener(new MessageListener<RangeCacheInvalidationMessage<K>>()
		{
			@Override
			public void onMessage(Message<RangeCacheInvalidationMessage<K>> message)
			{
				final RangeCacheInvalidationMessage<K> val = message.getMessageObject();
				if (val.isAll())
				{
					delegate.clear();
				}
				else
				{
					for (Range<K> range : val.getKeys())
					{
						delegate.remove(range);						
					}
				}
			}
		});
		this.delegate = rangeCache;
	}
	
	@Override
	public V get(K key)
	{
		return delegate.get(key);
	}

	@Override
	public Entry<Range<K>, V> getEntry(K key)
	{
		return delegate.getEntry(key);
	}

	@Override
	public Range<K> span()
	{
		return delegate.span();
	}

	@Override
	public void put(Range<K> range, V value)
	{
		this.put(range, value, 0);
	}

	@Override
	public void clear()
	{
		delegate.clear();
		topic.publish(new RangeCacheInvalidationMessage<K>());
	}

	@Override
	public void remove(Range<K> range)
	{
		delegate.remove(range);
		topic.publish(new RangeCacheInvalidationMessage<K>(range));
	}

	@Override
	public long getRangeCount()
	{
		return delegate.getRangeCount();
	}

	@Override
	public void put(Range<K> range, V value, long ttl)
	{
		delegate.put(range, value, ttl);
		topic.publish(new RangeCacheInvalidationMessage<K>(range));
	}
}
