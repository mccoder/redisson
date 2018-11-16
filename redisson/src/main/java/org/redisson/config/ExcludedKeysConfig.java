package org.redisson.config;

import java.util.HashSet;
import java.util.Set;

public class ExcludedKeysConfig {

    private final Set<String> keys = new HashSet<>();

    public void add(String key) {
        if (key != null && key.length() > 0) {
            keys.add(key.toLowerCase());
        }
    }

    public boolean contains(String key) {
        return key != null && keys.contains(key.toLowerCase());
    }

}
