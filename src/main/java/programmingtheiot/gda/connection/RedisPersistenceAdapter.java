/**
 * This class is part of the Programming the Internet of Things
 * project, and is available via the MIT License, which can be
 * found in the LICENSE file at the top level of this repository.
 * 
 * You may find it more helpful to your design to adjust the
 * functionality, constants and interfaces (if there are any)
 * provided within in order to meet the needs of your specific
 * Programming the Internet of Things project.
 */

package programmingtheiot.gda.connection;

import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import programmingtheiot.common.ConfigConst;
import programmingtheiot.common.ResourceNameEnum;
import programmingtheiot.data.ActuatorData;
import programmingtheiot.data.DataUtil;
import programmingtheiot.data.SensorData;
import programmingtheiot.data.SystemPerformanceData;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

/**
 * Shell representation of class for student implementation.
 * 
 */
public class RedisPersistenceAdapter implements IPersistenceClient
{
	// static
	private static final Logger _Logger =
		Logger.getLogger(RedisPersistenceAdapter.class.getName());
	
	// private var's
	private String host = ConfigConst.DEFAULT_HOST;
    private int port = 6380;  
	private JedisPool pool = null;
	private boolean isConnected = false;
	private volatile boolean isSubscribed = false;
	// constructors
	
	/**
	 * Default.
	 * 
	 */
	public RedisPersistenceAdapter()
	{
		super();
		initConfig();
		initPool();

		_Logger.info("Redis config loaded. host=" + this.host + ", port=" + this.port);
	}
	
	
	// public methods
	
	// public methods
	
	/**
	 *
	 */
	@Override
	public boolean connectClient()
	{
		if (this.pool == null || this.pool.isClosed()) {
			initPool();
		}
		if (this.pool == null) {
			this.isConnected = false;
			return false;
		}

		if (this.isConnected) {
			_Logger.warning("Redis client already connected.");
			return true;
		}

		try (Jedis j = this.pool.getResource()) {
			String pong = j.ping();
			this.isConnected = (pong != null && "PONG".equalsIgnoreCase(pong));

			if (this.isConnected) {
				_Logger.info("Connected to Redis successfully.");
			} else {
				_Logger.severe("Failed to connect to Redis. ping returned: " + pong);
			}

			return this.isConnected;
		} catch (Exception e) {
			_Logger.log(Level.SEVERE, "Failed to connect to Redis.", e);
			this.isConnected = false;
			return false;
		}
	}

	/**
	 *
	 */
	@Override
	public boolean disconnectClient()
	{
		if (!this.isConnected) {
			_Logger.warning("Redis client is already disconnected.");
			return true;
		}

		this.isConnected = false;
		_Logger.info("Disconnected from Redis.");
		return true;
	}

	/**
	 *
	 */
	@Override
	public ActuatorData[] getActuatorData(String topic, Date startDate, Date endDate)
	{
		if (topic == null || topic.isEmpty()) return new ActuatorData[0];
		if (!ensureReady()) return new ActuatorData[0];
		long start = (startDate != null) ? startDate.getTime() : 0L;
		long end   = (endDate != null) ? endDate.getTime() : System.currentTimeMillis();

		try (Jedis j = this.pool.getResource()) {
			List<String> jsonList = j.zrangeByScore(topic, start, end);

			ActuatorData[] arr = new ActuatorData[jsonList.size()];
			for (int i = 0; i < jsonList.size(); i++) {
				arr[i] = DataUtil.getInstance().jsonToActuatorData(jsonList.get(i));
			}
			return arr;
		} catch (Exception e) {
			_Logger.log(Level.WARNING, "getActuatorData failed.", e);
			return new ActuatorData[0];
		}
	}

	/**
	 *
	 */
	@Override
	public SensorData[] getSensorData(String topic, Date startDate, Date endDate)
	{
		if (topic == null || topic.isEmpty()) return new SensorData[0];
		if (!ensureReady()) return new SensorData[0];
		long start = (startDate != null) ? startDate.getTime() : 0L;
		long end   = (endDate != null) ? endDate.getTime() : System.currentTimeMillis();

		try (Jedis j = this.pool.getResource()) {
			List<String> jsonList = j.zrangeByScore(topic, start, end);

			SensorData[] arr = new SensorData[jsonList.size()];
			for (int i = 0; i < jsonList.size(); i++) {
				arr[i] = DataUtil.getInstance().jsonToSensorData(jsonList.get(i));
			}
			return arr;
		} catch (Exception e) {
			_Logger.log(Level.WARNING, "getSensorData failed.", e);
			return new SensorData[0];
		}
	}

	/**
	 *
	 */
	@Override
	public void registerDataStorageListener(Class cType, IPersistenceListener listener, String... topics)
	{
	}

	/**
	 *
	 */
	@Override
	public boolean storeData(String topic, int qos, ActuatorData... data)
	{
		if (topic == null || topic.isEmpty() || data == null || data.length == 0) return false;
		if (!ensureReady()) return false;
		try (Jedis j = this.pool.getResource()) {
			for (ActuatorData d : data) {
				if (d == null) continue;
				String json = DataUtil.getInstance().actuatorDataToJson(d);
				long ts = d.getTimeStampMillis();

				j.zadd(topic, ts, json);     
				j.publish(topic, json);      
			}
			return true;
		} catch (Exception e) {
			_Logger.log(Level.WARNING, "storeData(ActuatorData) failed.", e);
			return false;
		}

	}

	/**
	 *
	 */
	@Override
	public boolean storeData(String topic, int qos, SensorData... data)
	{
		if (topic == null || topic.isEmpty() || data == null || data.length == 0) return false;
		if (!ensureReady()) return false;
		try (Jedis j = this.pool.getResource()) {
			for (SensorData d : data) {
				if (d == null) continue;
				String json = DataUtil.getInstance().sensorDataToJson(d);
				long ts = d.getTimeStampMillis();

				j.zadd(topic, ts, json);     
				j.publish(topic, json);      
			}
			return true;
		} catch (Exception e) {
			_Logger.log(Level.WARNING, "storeData(SensorData) failed.", e);
			return false;
		}
	}

	/**
	 *
	 */
	@Override
	public boolean storeData(String topic, int qos, SystemPerformanceData... data)
	{
		if (topic == null || topic.isEmpty() || data == null || data.length == 0) return false;
		if (!ensureReady()) return false;
		try (Jedis j = this.pool.getResource()) {
			for (SystemPerformanceData d : data) {
				if (d == null) continue;
				String json = DataUtil.getInstance().systemPerformanceDataToJson(d);
				long ts = d.getTimeStampMillis();

				j.zadd(topic, ts, json);
				j.publish(topic, json);
			}
			return true;
		} catch (Exception e) {
			_Logger.log(Level.WARNING, "storeData(SystemPerformanceData) failed.", e);
			return false;
		}
	}
	
	public void subscribeToChannel(final JedisPubSub subscriber, final ResourceNameEnum resource)
	{
		if (isSubscribed) {
			return;
		}
		isSubscribed = true;
		if (subscriber == null || resource == null) return;

		final String channel = resource.getResourceName();
		if (channel == null || channel.isBlank()) return;
		if (!ensureReady()) return;
		new Thread(() -> {
			try (Jedis j = this.pool.getResource()) {
				_Logger.info("Starting Redis subscribe thread. channel:" + channel);
				j.subscribe(subscriber, channel);
			} catch (Exception e) {
				_Logger.log(Level.SEVERE, "Redis subscribe failed.", e);
			}
		}).start();
	}

	// private methods
	
	/**
	 * 
	 */

	private void initPool()
	{
	try {
		JedisPoolConfig cfg = new JedisPoolConfig();
		cfg.setMaxTotal(8);
		cfg.setMaxIdle(8);
		cfg.setMinIdle(0);
		cfg.setTestOnBorrow(true);
		cfg.setTestWhileIdle(true);

		if (this.pool != null && !this.pool.isClosed()) {
			this.pool.close();
		}

		this.pool = new JedisPool(cfg, this.host, this.port);
	} catch (Exception e) {
		_Logger.log(Level.SEVERE, "Failed to initialize JedisPool.", e);
		this.pool = null;
		this.isConnected = false;
	}
}	

	private void initConfig()
	{
	}

	private boolean ensureReady()
	{

		if (!this.isConnected) {
			return false;
		}

		if (this.pool == null || this.pool.isClosed()) {
			initPool();
			return (this.pool != null && !this.pool.isClosed());
		}

		return true;
	}

}
