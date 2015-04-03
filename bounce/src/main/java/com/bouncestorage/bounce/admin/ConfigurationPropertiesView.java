/*
 * Copyright (c) Bounce Storage, Inc. All rights reserved.
 * For more information, please see COPYRIGHT in the top-level directory.
 */

package com.bouncestorage.bounce.admin;

import static java.util.Objects.requireNonNull;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;

import org.apache.commons.configuration.Configuration;

/**
 * this provides a read only <code>Properties</code> view for a
 * <code>Configuration</code>.
 */
@SuppressWarnings("serial")
public final class ConfigurationPropertiesView extends Properties {
    private final Configuration config;

    ConfigurationPropertiesView(Configuration config) {
        this.config = requireNonNull(config);
    }

    @Override
    public synchronized int size() {
        return Iterators.size(config.getKeys());
    }

    @Override
    public synchronized boolean isEmpty() {
        return config.isEmpty();
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized Enumeration<Object> keys() {
        Enumeration<String> e = Iterators.asEnumeration(config.getKeys());
        return (Enumeration<Object>) (Enumeration<?>) e;
    }

    @Override
    public synchronized Enumeration<Object> elements() {
        final Iterator<String> keys = config.getKeys();
        return new Enumeration<Object>() {
            @Override
            public boolean hasMoreElements() {
                return keys.hasNext();
            }

            @Override
            public Object nextElement() {
                return config.getString(keys.next());
            }
        };
    }

    @Override
    public synchronized boolean contains(Object value) {
        return ImmutableSet.copyOf(Iterators.forEnumeration(elements()))
                .contains(value);
    }

    @Override
    public boolean containsValue(Object value) {
        return contains(value);
    }

    @Override
    public synchronized boolean containsKey(Object key) {
        return config.containsKey((String) key);
    }

    @Override
    public synchronized Object get(Object key) {
        return config.getString((String) key);
    }

    @Override
    public synchronized Object put(Object key, Object value) {
        throw new UnsupportedOperationException("this properties cannot be modified");
    }

    @Override
    public synchronized Object remove(Object key) {
        throw new UnsupportedOperationException("this properties cannot be modified");
    }

    @Override
    public synchronized void putAll(Map<?, ?> t) {
        throw new UnsupportedOperationException("this properties cannot be modified");
    }

    @Override
    public synchronized void clear() {
        throw new UnsupportedOperationException("this properties cannot be modified");
    }

    @Override
    public Set<Object> keySet() {
        return ImmutableSet.copyOf(config.getKeys());
    }

    @Override
    public Set<Map.Entry<Object, Object>> entrySet() {
        return new Set<Map.Entry<Object, Object>>() {

            @Override
            public int size() {
                return ConfigurationPropertiesView.this.size();
            }

            @Override
            public boolean isEmpty() {
                return ConfigurationPropertiesView.this.isEmpty();
            }

            @Override
            public boolean contains(Object o) {
                @SuppressWarnings("unchecked")
                Map.Entry<Object, Object> entry = (Map.Entry<Object, Object>) o;
                Object value = ConfigurationPropertiesView.this.get(entry.getKey());
                return Objects.equals(value, entry.getValue());
            }

            @Override
            public Iterator<Map.Entry<Object, Object>> iterator() {
                final Iterator<String> keys = config.getKeys();
                return new Iterator<Map.Entry<Object, Object>>() {
                    @Override
                    public boolean hasNext() {
                        return keys.hasNext();
                    }

                    @Override
                    public Map.Entry<Object, Object> next() {
                        String k = keys.next();
                        Object v = ConfigurationPropertiesView.this.get(k);
                        return new AbstractMap.SimpleImmutableEntry<>(k, v);
                    }
                };
            }

            @Override
            public Object[] toArray() {
                return Iterators.toArray(iterator(), Object.class);
            }

            @Override
            @SuppressWarnings("unchecked")
            public <T> T[] toArray(T[] a) {
                Class<T> tClass = (Class<T>) a.getClass().getComponentType();
                Iterator<Map.Entry<Object, Object>> iter = iterator();
                if (a.length >= size()) {
                    int i = 0;
                    while (iter.hasNext()) {
                        a[i++] = tClass.cast(iter.next());
                    }
                    return a;
                } else {
                    return Iterators.toArray((Iterator<T>) iter, tClass);
                }
            }

            @Override
            public boolean add(Map.Entry<Object, Object> entry) {
                throw new UnsupportedOperationException("this properties cannot be modified");
            }

            @Override
            public boolean remove(Object o) {
                throw new UnsupportedOperationException("this properties cannot be modified");
            }

            @Override
            public boolean containsAll(Collection<?> c) {
                return false;
            }

            @Override
            public boolean addAll(Collection<? extends Map.Entry<Object, Object>> c) {
                throw new UnsupportedOperationException("this properties cannot be modified");
            }

            @Override
            public boolean retainAll(Collection<?> c) {
                throw new UnsupportedOperationException("this properties cannot be modified");
            }

            @Override
            public boolean removeAll(Collection<?> c) {
                throw new UnsupportedOperationException("this properties cannot be modified");
            }

            @Override
            public void clear() {
                throw new UnsupportedOperationException("this properties cannot be modified");
            }
        };
    }

    @Override
    public Collection<Object> values() {
        return super.values();
    }
}
