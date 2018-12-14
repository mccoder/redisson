/**
 * Copyright 2018 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.redisson.tomcat;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.catalina.session.StandardSession;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;
import org.redisson.api.RMap;
import org.redisson.api.RTopic;
import org.redisson.client.RedisException;
import org.redisson.tomcat.RedissonSessionManager.ReadMode;
import org.redisson.tomcat.RedissonSessionManager.UpdateMode;

/**
 * Redisson Session object for Apache Tomcat
 *
 * @author Nikita Koksharov
 */
public class RedissonSession extends StandardSession {

    private static final Log LOG = LogFactory.getLog(RedissonSession.class);

    private static final String IS_NEW_ATTR = "session:isNew";
    private static final String IS_VALID_ATTR = "session:isValid";
    private static final String THIS_ACCESSED_TIME_ATTR = "session:thisAccessedTime";
    private static final String MAX_INACTIVE_INTERVAL_ATTR = "session:maxInactiveInterval";
    private static final String LAST_ACCESSED_TIME_ATTR = "session:lastAccessedTime";
    private static final String CREATION_TIME_ATTR = "session:creationTime";

    public static final Set<String> ATTRS = new HashSet<String>(Arrays.asList(IS_NEW_ATTR, IS_VALID_ATTR,
        THIS_ACCESSED_TIME_ATTR, MAX_INACTIVE_INTERVAL_ATTR, LAST_ACCESSED_TIME_ATTR, CREATION_TIME_ATTR));

    private final RedissonSessionManager redissonManager;
    private final Map<String, Object> attrs;
    private final ExcludedKeysConfig excludeKeys;
    private final boolean existExcludedKeys;
    private final boolean redisReadMode;
    private RMap<String, Object> map;
    private RTopic topic;
    private final RedissonSessionManager.ReadMode readMode;
    private final UpdateMode updateMode;
    private Map<String, Object> requestCache;

    public RedissonSession(RedissonSessionManager manager, RedissonSessionManager.ReadMode readMode,
        UpdateMode updateMode) {
        super(manager);
        this.redissonManager = manager;
        this.readMode = readMode;
        this.redisReadMode = readMode == ReadMode.REDIS;
        this.updateMode = updateMode;

        try {
            Field attr = StandardSession.class.getDeclaredField("attributes");
            attrs = (Map<String, Object>) attr.get(this);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }

        this.excludeKeys = redissonManager.getExcludedKeys();
        this.existExcludedKeys = excludeKeys != null;

        if (readMode == ReadMode.CACHE_FIRST_THAN_REDIS) {
            requestCache = new HashMap<>();
        }
    }

    private static final long serialVersionUID = -2518607181636076487L;

    private boolean skipWriteKeyInRedis(String key) {
        return existExcludedKeys && excludeKeys.contains(key);
    }

    @Override
    public Object getAttribute(String name) {
        if (!skipWriteKeyInRedis(name)) {
            if (requestCache != null) {
                final Object result = requestCache.get(name);
                if (result != null) {
                    //LOG.info("Read from cache '" + name + "'");
                    return result;
                }
            }
            if (redisReadMode) {
                //If in session contains non serializable object
                //Return object from session, except Redis.
                final Object resFromSession = super.getAttribute(name);
                if (resFromSession != null && !(resFromSession instanceof Serializable)) {
                    LOG.warn("Read non serializable from session '" + name + "' " + resFromSession.getClass());
                    return resFromSession;
                }
                try {
                    final Object result = map.get(name);
                    if (requestCache != null) {
                        requestCache.put(name, result);
                    }
                    if (result != null) {
                        //LOG.info("Read from redis '" + name + "' " + result.getClass());
                    }
                    return result;
                } catch (RedisException e) {
                    LOG.error("Error read from redis '" + name + "'", e);
                }
            }
        }
        return super.getAttribute(name);
    }

    @Override
    public void setId(String id, boolean notify) {
        super.setId(id, notify);
        map = redissonManager.getMap(id);
        topic = redissonManager.getTopic();
    }

    public void delete() {
        map.delete();
        if (readMode == ReadMode.MEMORY) {
            topic.publish(new AttributesClearMessage(redissonManager.getNodeId(), getId()));
        }
        map = null;
    }

    @Override
    public void setCreationTime(long time) {
        super.setCreationTime(time);

        if (map != null) {
            Map<String, Object> newMap = new HashMap<String, Object>(3);
            newMap.put(CREATION_TIME_ATTR, creationTime);
            newMap.put(LAST_ACCESSED_TIME_ATTR, lastAccessedTime);
            newMap.put(THIS_ACCESSED_TIME_ATTR, thisAccessedTime);
            map.putAll(newMap);
            if (readMode == ReadMode.MEMORY) {
                topic.publish(createPutAllMessage(newMap));
            }
        }
    }

    @Override
    public void access() {
        super.access();

        if (map != null) {
            Map<String, Object> newMap = new HashMap<String, Object>(2);
            newMap.put(LAST_ACCESSED_TIME_ATTR, lastAccessedTime);
            newMap.put(THIS_ACCESSED_TIME_ATTR, thisAccessedTime);
            map.putAll(newMap);
            if (readMode == ReadMode.MEMORY) {
                topic.publish(createPutAllMessage(newMap));
            }
            if (getMaxInactiveInterval() >= 0) {
                map.expire(getMaxInactiveInterval(), TimeUnit.SECONDS);
            }
        }
    }

    protected AttributesPutAllMessage createPutAllMessage(Map<String, Object> newMap) {
        Map<String, Object> map = new HashMap<String, Object>();
        for (Entry<String, Object> entry : newMap.entrySet()) {
            map.put(entry.getKey(), entry.getValue());
        }
        return new AttributesPutAllMessage(redissonManager.getNodeId(), getId(), map);
    }

    @Override
    public void setMaxInactiveInterval(int interval) {
        super.setMaxInactiveInterval(interval);

        if (map != null) {
            fastPut(MAX_INACTIVE_INTERVAL_ATTR, maxInactiveInterval);
            if (maxInactiveInterval >= 0) {
                map.expire(getMaxInactiveInterval(), TimeUnit.SECONDS);
            }
        }
    }

    private void fastPut(String name, Object value) {
        if (requestCache != null) {
            Object oldValue = requestCache.get(name);
            if (oldValue != null && Objects.equals(oldValue, value)) {
                //LOG.info("WriteSkip " + name);
                return;
            }
            requestCache.put(name, value);
        }
//        if (!name.startsWith("session:")) {
//            LOG.info("Write '" + name + "'=[" + (value instanceof Serializable) + "] " + value.getClass());
//        }
        try {
            map.fastPut(name, value);
            if (readMode == ReadMode.MEMORY) {
                topic.publish(new AttributeUpdateMessage(redissonManager.getNodeId(), getId(), name, value));
            }
        } catch (Exception e) {
            LOG.error("Can't write '" + name + "'=[" + (value instanceof Serializable) + "] " + value.getClass(), e);
        }
    }

    @Override
    public void setValid(boolean isValid) {
        super.setValid(isValid);

        if (map != null) {
            if (!isValid && !map.isExists()) {
                return;
            }

            fastPut(IS_VALID_ATTR, isValid);
        }
    }

    @Override
    public void setNew(boolean isNew) {
        super.setNew(isNew);

        if (map != null) {
            fastPut(IS_NEW_ATTR, isNew);
        }
    }

    @Override
    public void endAccess() {
        boolean oldValue = isNew;
        super.endAccess();

        if (isNew != oldValue) {
            fastPut(IS_NEW_ATTR, isNew);
        }
    }

    public void superSetAttribute(String name, Object value, boolean notify) {
        super.setAttribute(name, value, notify);
    }

    @Override
    public void setAttribute(String name, Object value, boolean notify) {
        super.setAttribute(name, value, notify);

        if (value != null && !skipWriteKeyInRedis(name) && map != null) {
            if (value instanceof Serializable) {
                fastPut(name, value);
            } else if (value != null) {
                System.out.println("Redisson skip not serializable " + name + " " + value.getClass().getName());
            }
        }
    }

    public void superRemoveAttributeInternal(String name, boolean notify) {
        super.removeAttributeInternal(name, notify);
    }

    @Override
    protected void removeAttributeInternal(String name, boolean notify) {
        super.removeAttributeInternal(name, notify);

        if (updateMode == UpdateMode.DEFAULT && map != null) {
            map.fastRemove(name);
            if (readMode == ReadMode.MEMORY) {
                topic.publish(new AttributeRemoveMessage(redissonManager.getNodeId(), getId(), name));
            }
        }
    }

    public void clearRequestCache() {
        if (requestCache != null) {
            requestCache.clear();
        }
    }

    public void save() {
        clearRequestCache();

        Map<String, Object> newMap = new HashMap<String, Object>();
        newMap.put(CREATION_TIME_ATTR, creationTime);
        newMap.put(LAST_ACCESSED_TIME_ATTR, lastAccessedTime);
        newMap.put(THIS_ACCESSED_TIME_ATTR, thisAccessedTime);
        newMap.put(MAX_INACTIVE_INTERVAL_ATTR, maxInactiveInterval);
        newMap.put(IS_VALID_ATTR, isValid);
        newMap.put(IS_NEW_ATTR, isNew);

        if (attrs != null) {
            List<String> savedKeys = new ArrayList<>();

            for (Entry<String, Object> entry : attrs.entrySet()) {
                final String key = entry.getKey();
                final Object value = entry.getValue();
                if (skipWriteKeyInRedis(key) || !(value instanceof Serializable)) {
                    continue;
                }
                newMap.put(key, value);
                savedKeys.add(key);

                //LOG.info("Write '" + key + "'=[" + (value instanceof Serializable) + "] " + value.getClass());
            }

        }

        map.putAll(newMap);
        if (readMode == ReadMode.MEMORY) {
            topic.publish(createPutAllMessage(newMap));
        }

        if (maxInactiveInterval >= 0) {
            map.expire(getMaxInactiveInterval(), TimeUnit.SECONDS);
        }
    }

    public void load(Map<String, Object> attrs) {
        Long creationTime = (Long) attrs.remove(CREATION_TIME_ATTR);
        if (creationTime != null) {
            this.creationTime = creationTime;
        }
        Long lastAccessedTime = (Long) attrs.remove(LAST_ACCESSED_TIME_ATTR);
        if (lastAccessedTime != null) {
            this.lastAccessedTime = lastAccessedTime;
        }
        Integer maxInactiveInterval = (Integer) attrs.remove(MAX_INACTIVE_INTERVAL_ATTR);
        if (maxInactiveInterval != null) {
            this.maxInactiveInterval = maxInactiveInterval;
        }
        Long thisAccessedTime = (Long) attrs.remove(THIS_ACCESSED_TIME_ATTR);
        if (thisAccessedTime != null) {
            this.thisAccessedTime = thisAccessedTime;
        }
        Boolean isValid = (Boolean) attrs.remove(IS_VALID_ATTR);
        if (isValid != null) {
            this.isValid = isValid;
        }
        Boolean isNew = (Boolean) attrs.remove(IS_NEW_ATTR);
        if (isNew != null) {
            this.isNew = isNew;
        }

        for (Entry<String, Object> entry : attrs.entrySet()) {
            super.setAttribute(entry.getKey(), entry.getValue(), false);
        }
    }

}
