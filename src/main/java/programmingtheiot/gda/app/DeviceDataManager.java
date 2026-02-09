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

package programmingtheiot.gda.app;

import java.util.logging.Logger;

import programmingtheiot.common.ConfigConst;
import programmingtheiot.common.ConfigUtil;
import programmingtheiot.common.IActuatorDataListener;
import programmingtheiot.common.IDataMessageListener;
import programmingtheiot.common.ResourceNameEnum;
import programmingtheiot.data.ActuatorData;
import programmingtheiot.data.DataUtil;
import programmingtheiot.data.SensorData;
import programmingtheiot.data.SystemPerformanceData;
import programmingtheiot.data.SystemStateData;
import programmingtheiot.gda.connection.CoapServerGateway;
import programmingtheiot.gda.connection.IPersistenceClient;
import programmingtheiot.gda.connection.IPubSubClient;
import programmingtheiot.gda.connection.IRequestResponseClient;
import programmingtheiot.gda.connection.RedisPersistenceAdapter;
import programmingtheiot.gda.system.SystemPerformanceManager;
import redis.clients.jedis.JedisPubSub;
/**
 * Shell representation of class for student implementation.
 *
 */
public class DeviceDataManager extends JedisPubSub implements IDataMessageListener
{
	// static
	
	private static final Logger _Logger =
		Logger.getLogger(DeviceDataManager.class.getName());
	
	// private var's
	
	private boolean enableMqttClient = true;
	private boolean enableCoapServer = false;
	private boolean enableCloudClient = false;
	private boolean enableSmtpClient = false;
	private boolean enablePersistenceClient = false;
	
	private IActuatorDataListener actuatorDataListener = null;
	private IPubSubClient mqttClient = null;
	private IPubSubClient cloudClient = null;
	private IPersistenceClient persistenceClient = null;
	private IRequestResponseClient smtpClient = null;
	private CoapServerGateway coapServer = null;
	private boolean enableSystemPerf = false;
	private SystemPerformanceManager sysPerfMgr = null;	
	private RedisPersistenceAdapter redisClient = null;
	private volatile boolean isRedisSubscribed = false;
	// constructors
	
	public DeviceDataManager()
	{
		super();
		
		ConfigUtil configUtil = ConfigUtil.getInstance();
		
		this.enableMqttClient =
			configUtil.getBoolean(
				ConfigConst.GATEWAY_DEVICE, ConfigConst.ENABLE_MQTT_CLIENT_KEY);
		
		this.enableCoapServer =
			configUtil.getBoolean(
				ConfigConst.GATEWAY_DEVICE, ConfigConst.ENABLE_COAP_SERVER_KEY);
		
		this.enableCloudClient =
			configUtil.getBoolean(
				ConfigConst.GATEWAY_DEVICE, ConfigConst.ENABLE_CLOUD_CLIENT_KEY);
		
		this.enablePersistenceClient =
			configUtil.getBoolean(
				ConfigConst.GATEWAY_DEVICE, ConfigConst.ENABLE_PERSISTENCE_CLIENT_KEY);
		
		initManager();
	}
	
	public DeviceDataManager(
		boolean enableMqttClient,
		boolean enableCoapClient,
		boolean enableCloudClient,
		boolean enableSmtpClient,
		boolean enablePersistenceClient)
	{
		super();
		
		initConnections();
	}
	
	
	// public methods
	
	@Override
	public boolean handleActuatorCommandResponse(ResourceNameEnum resourceName, ActuatorData data)
	{
		if (data != null) {
			_Logger.info("Handling actuator response: " + data.getName());

			if (data.hasError()) {
				_Logger.warning("Error flag set for ActuatorData instance.");
			}

			if (this.redisClient != null) {
				String topic = (resourceName != null) ? resourceName.getResourceName() : null;
				if (topic != null) {
					this.redisClient.storeData(topic, ConfigConst.DEFAULT_QOS, data);
				}
			}
			return true;
		}
		return false;
	}

	@Override
	public boolean handleActuatorCommandRequest(ResourceNameEnum resourceName, ActuatorData data)
	{
		return false;
	}

	@Override
	public boolean handleIncomingMessage(ResourceNameEnum resourceName, String msg)
	{
		if (msg != null) {
			_Logger.info("Handling incoming generic message: " + msg);
			
			return true;
		} else {
			return false;
		}
	}

	private void handleIncomingDataAnalysis(ResourceNameEnum resourceName, ActuatorData data)
	{
		_Logger.fine("handleIncomingDataAnalysis(ActuatorData) called. resource=" +
			resourceName + ", data=" + ((data != null) ? data.getName() : "null"));
	}

	private void handleIncomingDataAnalysis(ResourceNameEnum resourceName, SystemStateData data)
	{
		_Logger.fine("handleIncomingDataAnalysis(SystemStateData) called. resource=" +
			resourceName + ", data=" + ((data != null) ? data.getName() : "null"));
	}

	private boolean handleUpstreamTransmission(ResourceNameEnum resourceName, String jsonData, int qos)
	{
		_Logger.fine("handleUpstreamTransmission() called. resource=" + resourceName +
			", qos=" + qos + ", jsonData.len=" + ((jsonData != null) ? jsonData.length() : 0));

		return false;
	}

	@Override
	public boolean handleSensorMessage(ResourceNameEnum resourceName, SensorData data)
	{
		if (data != null) {
			_Logger.info("Handling sensor message: " + data.getName());

			if (data.hasError()) {
				_Logger.warning("Error flag set for SensorData instance.");
			}

        if (this.redisClient != null && resourceName != null) {

            String channelTopic = resourceName.getResourceName();

            if (channelTopic != null) {
                String storeKey = channelTopic + ":store";

                this.redisClient.storeData(storeKey, ConfigConst.DEFAULT_QOS, data);
            }
        }

		return true;
    }

    	return false;
	}

	@Override
	public boolean handleSystemPerformanceMessage(ResourceNameEnum resourceName, SystemPerformanceData data)
	{
		if (data != null) {
			_Logger.info("Handling system performance message: " + data.getName());
			
			if (data.hasError()) {
				_Logger.warning("Error flag set for SystemPerformanceData instance.");
			}
			
			if (this.redisClient != null) {
				String topic = (resourceName != null) ? resourceName.getResourceName() : null;
				if (topic != null) {
					this.redisClient.storeData(topic, ConfigConst.DEFAULT_QOS, data);
				}
			}

			return true;
		}

		return false;
	}
	
	public void setActuatorDataListener(String name, IActuatorDataListener listener)
	{
	}
	
	public void startManager()
	{
		if (this.sysPerfMgr != null) {
			this.sysPerfMgr.startManager();
		}
	    if (this.redisClient != null) {
        	boolean ok = this.redisClient.connectClient();
			 _Logger.info("Redis connectClient(): " + ok);
			if (ok && !isRedisSubscribed) { 
				isRedisSubscribed = true;
				_Logger.info("Subscribing to Redis channel: " +
					ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE.getResourceName());

				this.redisClient.subscribeToChannel(this, ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE);
			}
		}
	}
	
	public void stopManager()
	{
		if (this.sysPerfMgr != null) {
			this.sysPerfMgr.stopManager();
		}
		if (this.redisClient != null) {
			this.redisClient.disconnectClient();
		}		
	}
	//Lab5 Optional
	@Override
	public void onSubscribe(String channel, int subscribedChannels)
	{
		_Logger.info("Redis subscribed. channel=" + channel + " count=" + subscribedChannels);
	}

	@Override
	public void onUnsubscribe(String channel, int subscribedChannels)
	{
		_Logger.info("Redis unsubscribed. channel=" + channel + " count=" + subscribedChannels);
	}

	@Override
	public void onMessage(String channel, String message)
	{
		_Logger.info("Redis msg received. channel=" + channel + " payload=" + message);
		
		if (channel == null || message == null) {
			return;
		}
		if (channel.equals(ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE.getResourceName())) {
			SensorData sd = DataUtil.getInstance().jsonToSensorData(message);

			if (sd != null) {
				this.handleSensorMessage(ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE, sd);
			} else {
				_Logger.warning("Failed to parse SensorData from JSON.");
			}
		}

		
	}	
	//Optional part ends

	// private methods
	
	/**
	 * Initializes the enabled connections. This will NOT start them, but only create the
	 * instances that will be used in the {@link #startManager() and #stopManager()) methods.
	 * 
	 */
	private void initConnections()
	{
	}

	private void initManager()
	{
		ConfigUtil configUtil = ConfigUtil.getInstance();
		
		this.enableSystemPerf =
			configUtil.getBoolean(ConfigConst.GATEWAY_DEVICE,  ConfigConst.ENABLE_SYSTEM_PERF_KEY);
		
		if (this.enableSystemPerf) {
			this.sysPerfMgr = new SystemPerformanceManager();
			this.sysPerfMgr.setDataMessageListener(this);
		}
		
		if (this.enableMqttClient) {
			// TODO: implement this in Lab Module 7
		}
		
		if (this.enableCoapServer) {
			// TODO: implement this in Lab Module 8
		}
		
		if (this.enableCloudClient) {
			// TODO: implement this in Lab Module 10
		}
		
		if (this.enablePersistenceClient) {
			this.redisClient = new RedisPersistenceAdapter();
    		_Logger.info("Redis Persistence client enabled.");
		}
	}	
}
