package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.core.KeyHelper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

public class RateLimiterSlidingDaoRedisImpl implements RateLimiter {

    private final JedisPool jedisPool;
    private final long windowSizeMS;
    private final long maxHits;

    public RateLimiterSlidingDaoRedisImpl(JedisPool pool, long windowSizeMS,
                                          long maxHits) {
        this.jedisPool = pool;
        this.windowSizeMS = windowSizeMS;
        this.maxHits = maxHits;
    }

    // Challenge #7
    @Override
    public void hit(String name) throws RateLimitExceededException {
        // START CHALLENGE #7
        String key = KeyHelper.getKey(getLimiterKey(name));
        try(Jedis jedis = jedisPool.getResource()){
            Transaction transaction = jedis.multi();
                transaction.zadd(key, System.currentTimeMillis(), getValue());
                long currentWindowsStart = System.currentTimeMillis() - windowSizeMS;
                transaction.zremrangeByScore(key, 0, currentWindowsStart);
                Response<Long> zcard = transaction.zcard(key);
            transaction.exec();

            if(zcard.get() > maxHits) {
                throw new RateLimitExceededException();
            }
        }
        // END CHALLENGE #7
    }

    private String getValue() {
        return String.format("%s-%s", System.currentTimeMillis(), Math.random());
    }

    private String getLimiterKey(String name) {
        return String.format("limiter:%s:%s:%s", windowSizeMS, name, maxHits);
    }
}
