package com.typesafe.plugin

import play.api._
import redis.clients.jedis._
import play.api.cache._
import java.io._
import biz.source_code.base64Coder._
import play.api.mvc.Result
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConversions._
import scala.util.control.NonFatal

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
        case Array(h, p) => new HostAndPort(h.trim(), p.trim().toInt)
        case Array(h) => new HostAndPort(h.trim(), port)
      }
    } toSet
  } getOrElse Set(new HostAndPort("localhost", port))

  private lazy val timeout = app.configuration.getInt("redis.timeout") getOrElse 2000

  lazy val jedis = new JedisCluster(hosts, timeout)

 override def onStart() {
    jedis
 }

 override def onStop() {
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
       
       jedis.set(key,redisV)
       if (expiration != 0) jedis.expire(key,expiration)
     } catch {
       case ex: IOException => Logger.warn("could not serialize key:"+ key + " and value:"+ value.toString + " ex:"+ex.toString)
       case NonFatal(ex) => Logger.error("cache error", ex)
     } finally {
       if (oos != null) oos.close()
       if (dos != null) dos.close()
     }

    }
    def remove(key: String): Unit = try {
      jedis.del(key)
    } catch {
      case NonFatal(ex) => Logger.error("cache error", ex)
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
        val rawData = jedis.get(key)
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
        case NonFatal(ex) =>
          Logger.error("cache error", ex)
          None
      }
    }

  }
}
