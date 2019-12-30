package Redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class Redistest {
    public static void main(String[] args) {
        //1.创建redis连接对象
        Jedis jedis = new Jedis("master",6379);
        //向Redis中存储数据
        jedis.set("s1","redis test");
        //获取rdis存储的数据
        String s = jedis.get("s1");
        System.out.println(s);
        //2.创建jedisPool
        JedisPool pool = new JedisPool("master",6379);
        //获取连接池实例
        Jedis jedis1 = pool.getResource();
        jedis1.set("s2","jedis pool");
        String ss = jedis1.get("s2");
        //需关闭连接对象和池
        jedis1.close();
        jedis.close();
        pool.close();
    }
}
