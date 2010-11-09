package org.apache.hadoop.mapred.buffer.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class SortedByValueMap<K extends Object, V extends Comparable<V>>
{
	/**
	 * 
	 */
	TreeMap<V, K> innerMap;

	public SortedByValueMap()
	{
		innerMap = new TreeMap<V, K>();
	}

	public V put(K key, V value)
	{
		// sorted by value of outer map
		V oldValue = get(key);
		if(oldValue != null){
			if(oldValue.compareTo(value)<0){
				innerMap.remove(value);
				innerMap.put(value, key);
			}
		}else{
			innerMap.put(value, key);
		}

		return null;
	}

	@SuppressWarnings("unchecked")
	public V get(K realKey)
	{
		Set entries = innerMap.entrySet();
		Iterator it = entries.iterator();
		while(it.hasNext())
		{
			Map.Entry<V, K> entry = (Map.Entry)it.next();
			if(entry.getValue().equals(realKey))
			{
				return entry.getKey();
			}
		}
		return null;
	}

	public void clear()
	{
		innerMap.clear();
	}

	@SuppressWarnings("unchecked")
	public boolean containsKey(K realKey)
	{
		Set entries = innerMap.entrySet();
		Iterator it = entries.iterator();
		while(it.hasNext())
		{
			Map.Entry<V, K> entry = (Map.Entry<V, K>)it.next();
			if(entry.getValue().equals(realKey))
			{
				return true;
			}
		}
		return false; 
	}

	@SuppressWarnings("unchecked")
	public Set entrySet()
	{
		Set entries = innerMap.entrySet();
		Iterator it = entries.iterator();
		HashMap<K, V> tempMap = new HashMap<K, V>();
		while(it.hasNext())
		{
			Map.Entry<V, K> entry = (Map.Entry<V, K>)it.next();
			tempMap.put(entry.getValue(), entry.getKey());
		}
		return tempMap.entrySet();
	} 

	@SuppressWarnings("unchecked")
	public Set entrySet(int size)
	{
		Set entries = innerMap.entrySet();
		Iterator it = entries.iterator();
		HashMap<K, V> tempMap = new HashMap<K, V>(size);
		int i = 0;
		while((i++<size) && (it.hasNext()))
		{
			Map.Entry<V, K> entry = (Map.Entry<V, K>)it.next();
			tempMap.put(entry.getValue(), entry.getKey());
		}
		return tempMap.entrySet();
	}
	
	public boolean isEmpty()
	{
		return innerMap.isEmpty();
	}

	@SuppressWarnings("unchecked")
	public K remove(K realKey)
	{
		Set entries = innerMap.entrySet();
		Iterator it = entries.iterator();
		while(it.hasNext())
		{
			Map.Entry<V, K> entry = (Map.Entry<V, K>)it.next();
			if(entry.getValue().equals(realKey))
			{
				V key = entry.getKey();
				return innerMap.remove(key);
			}
		}
		return null;
	}


	public Collection<V> values(int size)
	{	
		/*
		List<V> mapValues = new ArrayList<V>();
		Set entries = innerMap.entrySet();
		Iterator it = entries.iterator();
		int i = 0;
		while((i<size) && (it.hasNext()))
		{
			Map.Entry<V, K> entry = (Map.Entry<V, K>)it.next();
			mapValues.add(entry.getKey());
			i++;
		}
		return mapValues;
		*/
		List<V> mapValues = new ArrayList<V>();
		Set<V> entries = innerMap.descendingKeySet();
		Iterator<V> itr = entries.iterator();
		int i = 0;
		while((i<size) && (itr.hasNext()))
		{
			V entry = itr.next();
			mapValues.add(entry);
			i++;
		}
		return mapValues;
	}

	@SuppressWarnings("unchecked")
	public Set<K> keySet()
	{
		Set<K> mapKeys = new TreeSet<K>();
		Set entries = innerMap.entrySet();
		Iterator it = entries.iterator();
		while(it.hasNext())
		{
			Map.Entry<V, K> entry = (Map.Entry<V, K>)it.next();
			mapKeys.add(entry.getValue());
		}
		return mapKeys;
	}
	
	public V pollFirst() {
		Map.Entry<V, K> first = this.innerMap.pollLastEntry();
		return (first == null) ? null : first.getKey();
	}
	
	public int size() {
		return this.innerMap.size();
	}
} 
