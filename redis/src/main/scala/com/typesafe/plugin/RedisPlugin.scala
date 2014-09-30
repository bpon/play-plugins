package com.typesafe.plugin

import org.apache.commons.lang3.builder.ReflectionToStringBuilder
import play.api._
import redis.clients.jedis._
import play.api.cache._
import java.io._
import biz.source_code.base64Coder._
import play.api.mvc.Result
import redis.clients.jedis.exceptions.JedisException
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConversions._

/**
 * provides a redis client and a CachePlugin implementation
 * the cache implementation can deal with the following data types:
 * - classes implement Serializable
 * - String, Int, Boolean and long
 */
class RedisPlugin(app: Application) extends CachePlugin {

  private lazy val port = app.configuration.getInt("redis.port") getOrElse 6379

  //host:port
  private lazy val hosts = app.configuration.getString("redis.hosts") map {
    _.split(",") map {
      _.split(":", 2) match {
        case Array(h, p) => new JedisShardInfo(h.trim(), p.trim().toInt, timeout)
        case Array(h) => new JedisShardInfo(h.trim(), port, timeout)
      }
    } toList
  } getOrElse List(new JedisShardInfo("localhost", port, timeout))

  private lazy val timeout = app.configuration.getInt("redis.timeout") getOrElse 2000

  lazy val jedisPool = {
    val poolConfig = createPoolConfig(app)
    Logger.info(s"Redis Plugin enabled. Connecting to Redis on [${hosts.map(x => s"${x.getHost}:${x.getPort}").mkString(", ")}].")
    Logger.info("Redis Plugin pool configuration: " + new ReflectionToStringBuilder(poolConfig).toString())
    new ShardedJedisPool(poolConfig, hosts)
  }

  def withJedisClient[T](f: ShardedJedis => T): T = {
    val jedis = jedisPool.getResource
    try {
      f(jedis)
    } finally {
      jedisPool.returnResourceObject(jedis)
    }
  }

  private def createPoolConfig(app: Application) : JedisPoolConfig = {
    val poolConfig : JedisPoolConfig = new JedisPoolConfig()
    app.configuration.getInt("redis.pool.maxIdle").map { poolConfig.setMaxIdle(_) }
    app.configuration.getInt("redis.pool.minIdle").map { poolConfig.setMinIdle(_) }
    app.configuration.getInt("redis.pool.maxActive").map { x => poolConfig.setMaxTotal(x + poolConfig.getMaxIdle()) }
    app.configuration.getInt("redis.pool.maxWait").map { poolConfig.setMaxWaitMillis(_) }
    app.configuration.getBoolean("redis.pool.testOnBorrow").map { poolConfig.setTestOnBorrow(_) }
    app.configuration.getBoolean("redis.pool.testOnReturn").map { poolConfig.setTestOnReturn(_) }
    app.configuration.getBoolean("redis.pool.testWhileIdle").map { poolConfig.setTestWhileIdle(_) }
    app.configuration.getLong("redis.pool.timeBetweenEvictionRunsMillis").map { poolConfig.setTimeBetweenEvictionRunsMillis(_) }
    app.configuration.getInt("redis.pool.numTestsPerEvictionRun").map { poolConfig.setNumTestsPerEvictionRun(_) }
    app.configuration.getLong("redis.pool.minEvictableIdleTimeMillis").map { poolConfig.setMinEvictableIdleTimeMillis(_) }
    app.configuration.getLong("redis.pool.softMinEvictableIdleTimeMillis").map { poolConfig.setSoftMinEvictableIdleTimeMillis(_) }
    app.configuration.getBoolean("redis.pool.lifo").map { poolConfig.setLifo(_) }
    app.configuration.getBoolean("redis.pool.blockWhenExhausted").map { poolConfig.setBlockWhenExhausted(_) }
    poolConfig
  }

  override def onStart() {
    jedisPool
  }

  override def onStop(): Unit = {
    jedisPool.destroy()
  }

 override lazy val enabled = {
    !app.configuration.getString("redisplugin").filter(_ == "disabled").isDefined
  }

 /**
  * cacheAPI implementation
  * can serialize, deserialize to/from redis
  * value needs be Serializable (a few primitive types are also supported: String, Int, Long, Boolean)
  */
 lazy val api = new CacheAPI {

    def set(key: String, value: Any, expiration: Int) {
      value match {
        case result:Result =>
          RedisResult.wrapResult(result).map {
            redisResult => set_(key, redisResult, expiration)
          }
        case _ => set_(key, value, expiration)
      }
    }      
    
    def set_(key: String, value: Any, expiration: Int) {
     var oos: ObjectOutputStream = null
     var dos: DataOutputStream = null
     try {
       val baos = new ByteArrayOutputStream()
       var prefix = "oos"
       if (value.isInstanceOf[RedisResult]) {
          oos = new ObjectOutputStream(baos)
          oos.writeObject(value)
          oos.flush()
          prefix = "result"
       } else if (value.isInstanceOf[Serializable]) {
          oos = new ObjectOutputStream(baos)
          oos.writeObject(value)
          oos.flush()
       } else if (value.isInstanceOf[String]) {
          dos = new DataOutputStream(baos)
          dos.writeUTF(value.asInstanceOf[String])
          prefix = "string"
       } else if (value.isInstanceOf[Int]) {
          dos = new DataOutputStream(baos)
          dos.writeInt(value.asInstanceOf[Int])
          prefix = "int"
       } else if (value.isInstanceOf[Long]) {
          dos = new DataOutputStream(baos)
          dos.writeLong(value.asInstanceOf[Long])
          prefix = "long"
       } else if (value.isInstanceOf[Boolean]) {
          dos = new DataOutputStream(baos)
          dos.writeBoolean(value.asInstanceOf[Boolean])
          prefix = "boolean"
       } else {
          throw new IOException("could not serialize: "+ value.toString)
       }
       val redisV = prefix + "-" + new String( Base64Coder.encode( baos.toByteArray() ) )
       Logger.trace(s"Setting key ${key} to ${redisV}")

       withJedisClient { client =>
         client.set(key, redisV)
         if (expiration != 0) client.expire(key, expiration)
       }
     } catch {
       case ex: IOException => Logger.warn("could not serialize key:"+ key + " and value:"+ value.toString + " ex:"+ex.toString)
       case ex: JedisException => Logger.error("cache error", ex)
     } finally {
       if (oos != null) oos.close()
       if (dos != null) dos.close()
     }

    }
    def remove(key: String): Unit = try {
      withJedisClient(_.del(key))
    } catch {
      case ex: JedisException => Logger.error("cache error", ex)
    }

    class ClassLoaderObjectInputStream(stream:InputStream) extends ObjectInputStream(stream) {
      override protected def resolveClass(desc: ObjectStreamClass) = {
        Class.forName(desc.getName(), false, app.classloader)
      }
    }

    def withDataInputStream[T](bytes: Array[Byte])(f: DataInputStream => T): T = {
      val dis = new DataInputStream(new ByteArrayInputStream(bytes))
      try f(dis) finally dis.close()
    }

    def withObjectInputStream[T](bytes: Array[Byte])(f: ObjectInputStream => T): T = {
      val ois = new ClassLoaderObjectInputStream(new ByteArrayInputStream(bytes))
      try f(ois) finally ois.close()
    }

    def get(key: String): Option[Any] = {
      Logger.trace(s"Reading key ${key}")

      try {
        val rawData = withJedisClient(_.get(key))
        rawData match {
          case null =>
            None
          case _ =>
            val data: Seq[String] =  rawData.split("-")
            val bytes = Base64Coder.decode(data.last)
            data.head match {
              case "result" =>
                Some(RedisResult.unwrapResult(withObjectInputStream(bytes)(_.readObject())
                  .asInstanceOf[RedisResult]))
              case "oos" => Some(withObjectInputStream(bytes)(_.readObject()))
              case "string" => Some(withDataInputStream(bytes)(_.readUTF()))
              case "int" => Some(withDataInputStream(bytes)(_.readInt()))
              case "long" => Some(withDataInputStream(bytes)(_.readLong()))
              case "boolean" => Some(withDataInputStream(bytes)(_.readBoolean()))
              case _ => throw new IOException("can not recognize value")
            }
        }
      } catch {
        case ex: IOException =>
          Logger.warn("could not deserialize key: "+ key, ex)
          None
        case ex: JedisException =>
          Logger.error("cache error", ex)
          None
      }
    }

  }
}
