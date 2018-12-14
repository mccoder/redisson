package org.redisson.tomcat;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ExcludedKeysConfig {

    private static final String PREFIX_KEY = "$^";
    private final Set<String> keys = new HashSet<>();
    private final List<String> prefixKeys = new ArrayList<>();

    public void add(String key) {
        if (key != null && key.length() > 0) {
            final String keyLower = key.toLowerCase();
            if (keyLower.startsWith(PREFIX_KEY)) {
                prefixKeys.add(keyLower.substring(PREFIX_KEY.length()));
                System.out.println("Redisson prefix key: " + key);
            } else {
                keys.add(keyLower);
            }
        }
    }

    public boolean contains(final String key) {
        if (key != null) {
            final String lowerKey = key.toLowerCase();
            return keys.contains(lowerKey) || startedWithPrefix(lowerKey);
        }
        return false;
    }

    private boolean startedWithPrefix(final String lowerKey) {
        for (String prefix : prefixKeys) {
            if (lowerKey.startsWith(prefix)) {
                keys.add(lowerKey);
                System.out.println("Redisson Prefix skip:" + lowerKey);
                return true;
            }
        }
        return false;
    }

}
