package programmingtheiot.gda.app;

import java.time.OffsetDateTime;
import java.util.logging.Level;
import java.util.logging.Logger;

import programmingtheiot.common.ConfigConst;
import programmingtheiot.common.ConfigUtil;
import programmingtheiot.common.IActuatorDataListener;
import programmingtheiot.common.IDataMessageListener;
import programmingtheiot.common.ResourceNameEnum;
import programmingtheiot.data.ActuatorData;
import programmingtheiot.data.BaseIotData;
import programmingtheiot.data.DataUtil;
import programmingtheiot.data.SensorData;
import programmingtheiot.data.SystemPerformanceData;
import programmingtheiot.data.SystemStateData;
import programmingtheiot.gda.connection.CloudClientConnector;
import programmingtheiot.gda.connection.CoapClientConnector;
import programmingtheiot.gda.connection.CoapServerGateway;
import programmingtheiot.gda.connection.ICloudClient;
import programmingtheiot.gda.connection.IPersistenceClient;
import programmingtheiot.gda.connection.IPubSubClient;
import programmingtheiot.gda.connection.IRequestResponseClient;
import programmingtheiot.gda.connection.MqttClientConnector;
import programmingtheiot.gda.connection.RedisPersistenceAdapter;
import programmingtheiot.gda.system.SystemPerformanceManager;
import redis.clients.jedis.JedisPubSub;

public class DeviceDataManager extends JedisPubSub implements IDataMessageListener
{
	private static final Logger _Logger =
		Logger.getLogger(DeviceDataManager.class.getName());

	private boolean enableMqttClient = true;
	private boolean enableCoapServer = false;
	private boolean enableCloudClient = false;
	private boolean enableSmtpClient = false;
	private boolean enablePersistenceClient = false;
	private boolean enableCoapClient = false;

	private IActuatorDataListener actuatorDataListener = null;
	private IPubSubClient mqttClient = null;
	private ICloudClient cloudClient = null;
	private IPersistenceClient persistenceClient = null;
	private IRequestResponseClient smtpClient = null;
	private CoapServerGateway coapServer = null;
	private boolean enableSystemPerf = false;
	private SystemPerformanceManager sysPerfMgr = null;
	private RedisPersistenceAdapter redisClient = null;
	private volatile boolean isRedisSubscribed = false;
	private CoapClientConnector coapClient = null;

	private boolean handleHumidityChangeOnDevice = false;

	private int humidityMaxTimePastThreshold = 0;
	private float nominalHumiditySetting = 0.0f;
	private float triggerHumidifierFloor = 0.0f;
	private float triggerHumidifierCeiling = 0.0f;

	private SensorData lastHumiditySensorData = null;
	private ActuatorData lastHumidifierActuatorCommand = null;
	private ActuatorData lastHumidifierActuatorResponse = null;

	private long humidityThresholdStartTime = 0L;
	private boolean humidityAboveCeiling = false;
	private boolean humidityBelowFloor = false;

	private SensorData latestHumiditySensorData = null;
	private OffsetDateTime latestHumiditySensorTimeStamp = null;
	private ActuatorData latestHumidifierActuatorData = null;
	private int lastKnownHumidifierCommand = ConfigConst.DEFAULT_COMMAND;
	private static final int NO_PENDING_COMMAND = -1;
	private int pendingHumidifierCommand = NO_PENDING_COMMAND;

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

		this.enableCoapClient =
			configUtil.getBoolean(
				ConfigConst.GATEWAY_DEVICE,
				ConfigConst.ENABLE_COAP_CLIENT_KEY);

		this.handleHumidityChangeOnDevice =
			ConfigUtil.getInstance().getBoolean(
				ConfigConst.GATEWAY_DEVICE,
				"handleHumidityChangeOnDevice"
			);

		this.humidityMaxTimePastThreshold =
			ConfigUtil.getInstance().getInteger(
				ConfigConst.GATEWAY_DEVICE,
				"humidityMaxTimePastThreshold"
			);

		this.nominalHumiditySetting =
			ConfigUtil.getInstance().getFloat(
				ConfigConst.GATEWAY_DEVICE,
				"nominalHumiditySetting"
			);

		this.triggerHumidifierFloor =
			ConfigUtil.getInstance().getFloat(
				ConfigConst.GATEWAY_DEVICE,
				"triggerHumidifierFloor"
			);

		this.triggerHumidifierCeiling =
			ConfigUtil.getInstance().getFloat(
				ConfigConst.GATEWAY_DEVICE,
				"triggerHumidifierCeiling"
			);

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

	@Override
	public boolean handleActuatorCommandResponse(ResourceNameEnum resourceName, ActuatorData data)
	{
		if (data != null) {
			_Logger.info("Handling actuator response: " + data.getName());

			if (data.hasError()) {
				_Logger.warning("Error flag set for ActuatorData instance.");
			}

			if (isHumidifierActuatorData(data) && !data.hasError()) {
				this.lastHumidifierActuatorResponse = data;
				this.latestHumidifierActuatorData   = data;
				this.lastKnownHumidifierCommand      = data.getCommand();
				this.pendingHumidifierCommand        = NO_PENDING_COMMAND;
			}

			if (this.actuatorDataListener != null) {
				this.actuatorDataListener.onActuatorDataUpdate(data);
			}

			if (this.redisClient != null) {
				String topic = (resourceName != null) ? resourceName.getResourceName() : null;
				if (topic != null) {
					this.redisClient.storeData(topic, ConfigConst.DEFAULT_QOS, data);
				}
			}

			if (this.cloudClient != null && resourceName != null && !data.hasError()) {
				if (this.cloudClient.sendEdgeDataToCloud(resourceName, data)) {
					_Logger.fine("Sent ActuatorData state to cloud: " + data.getName() + "=" + data.getCommand());
				} else {
					_Logger.warning("Failed to send ActuatorData state to cloud.");
				}
			}

			return true;
		}
		return false;
	}
	
	@Override
	public boolean handleActuatorCommandRequest(ResourceNameEnum resourceName, ActuatorData data)
	{
		if (data != null) {
			_Logger.log(
				Level.FINE,
				"Actuator request received: {0}. Message: {1}",
				new Object[] {resourceName.getResourceName(), Integer.valueOf((data.getCommand()))});

			if (data.hasError()) {
				_Logger.warning("Error flag set for ActuatorData instance.");
			}

			this.sendActuatorCommandtoCda(resourceName, data);

			return true;
		} else {
			return false;
		}
	}

	@Override
	public boolean handleIncomingMessage(ResourceNameEnum resourceName, String msg)
	{
		if (resourceName != null && msg != null) {
			_Logger.info("Handling incoming ActuatorData message: " + msg);

			try {
				ActuatorData ad = DataUtil.getInstance().jsonToActuatorData(msg);

				if (ad == null) {
					_Logger.warning("Failed to parse incoming ActuatorData JSON: " + msg);
					return false;
				}

				String jsonData = DataUtil.getInstance().actuatorDataToJson(ad);

				if (this.mqttClient != null) {
					_Logger.info("Publishing data to MQTT broker: " + jsonData);

					boolean ok = this.mqttClient.publishMessage(
						ResourceNameEnum.CDA_ACTUATOR_CMD_RESOURCE,
						jsonData,
						0
					);

					_Logger.info("Actuator publish to CDA result: " + ok);

					return ok;
				} else {
					_Logger.warning("MQTT client is null. Cannot publish ActuatorData to CDA.");
				}
			} catch (Exception e) {
				_Logger.warning("Failed to process incoming message: " + e.getMessage());
			}
		}

		return false;
	}

	private void handleIncomingDataAnalysis(ResourceNameEnum resource, ActuatorData data)
	{
		_Logger.info("Analyzing incoming actuator data: " + data.getName());

		if (data.isResponseFlagEnabled()) {
			// TODO
		} else {
			if (this.actuatorDataListener != null) {
				this.actuatorDataListener.onActuatorDataUpdate(data);
			}
		}
	}

	private void handleIncomingDataAnalysis(ResourceNameEnum resourceName, SystemStateData data)
	{
		_Logger.fine("handleIncomingDataAnalysis(SystemStateData) called. resource=" +
			resourceName + ", data=" + ((data != null) ? data.getName() : "null"));
	}

	private void handleIncomingDataAnalysis(ResourceNameEnum resourceName, SensorData data)
	{
		if (data == null) {
			return;
		}

		if (data.getTypeID() == ConfigConst.HUMIDITY_SENSOR_TYPE) {
			handleHumiditySensorAnalysis(resourceName, data);
		}
	}

	private void handleUpstreamTransmission(ResourceNameEnum resource, SensorData data)
	{
		_Logger.fine("Sending SensorData upstream to cloud: " + resource);

		if (this.cloudClient != null && resource != null && data != null) {
			if (this.cloudClient.sendEdgeDataToCloud(resource, data)) {
				_Logger.fine("Sent SensorData upstream to cloud.");
			} else {
				_Logger.warning("Failed to send SensorData upstream to cloud.");
			}
		}
	}

	@Override
	public boolean handleSensorMessage(ResourceNameEnum resourceName, SensorData data)
	{
		if (data != null) {
			_Logger.info("Handling sensor message: " + data.getName());

			if (data.hasError()) {
				_Logger.warning("Error flag set for SensorData instance.");
			}

			try {
				handleIncomingDataAnalysis(resourceName, data);
			} catch (Exception e) {
				_Logger.warning("Failed to analyze SensorData. Message: " + e.getMessage());
			}

			if (this.redisClient != null && resourceName != null) {
				String channelTopic = resourceName.getResourceName();

				if (channelTopic != null) {
					String storeKey = channelTopic + ":store";
					this.redisClient.storeData(storeKey, ConfigConst.DEFAULT_QOS, data);
				}
			}

			if (this.coapClient != null && resourceName != null) {
				try {
					String jsonData = DataUtil.getInstance().sensorDataToJson(data);

					_Logger.info("Forwarding SensorData over CoAP PUT: " + jsonData);

					this.coapClient.sendPutRequest(
						resourceName,
						null,
						true,
						jsonData,
						5
					);
				} catch (Exception e) {
					_Logger.warning("Failed to forward SensorData over CoAP. Message: " + e.getMessage());
				}
			}

			handleUpstreamTransmission(resourceName, data);

			return true;
		}

		return false;
	}

	@Override
	public boolean handleSystemPerformanceMessage(ResourceNameEnum resourceName, SystemPerformanceData data)
	{
		if (data != null) {
			_Logger.info("Handling system performance message: " + data.getName());

			if (resourceName == ResourceNameEnum.CDA_SYSTEM_PERF_MSG_RESOURCE) {
				_Logger.info("Ignoring CDA system performance message in DeviceDataManager.");
				return true;
			}

			if (data.hasError()) {
				_Logger.warning("Error flag set for SystemPerformanceData instance.");
			}

			if (this.redisClient != null) {
				String topic = (resourceName != null) ? resourceName.getResourceName() : null;
				if (topic != null) {
					this.redisClient.storeData(topic, ConfigConst.DEFAULT_QOS, data);
				}
			}

			if (this.cloudClient != null) {
				if (this.cloudClient.sendEdgeDataToCloud(resourceName, data)) {
					_Logger.fine("Sent SystemPerformanceData upstream to cloud.");
				} else {
					_Logger.warning("Failed to send SystemPerformanceData upstream to cloud.");
				}
			}
			return true;
		}

		return false;
	}

	private void handleHumidityDataAnalysis(SensorData data)
	{
		if (!this.handleHumidityChangeOnDevice) {
			return;
		}

		float humidity = data.getValue();
		long now = System.currentTimeMillis();

		this.lastHumiditySensorData = data;

		_Logger.info("Humidity analysis - current value: " + humidity);

		if (humidity >= this.triggerHumidifierFloor && humidity <= this.triggerHumidifierCeiling) {
			this.humidityThresholdStartTime = 0L;
			this.humidityAboveCeiling = false;
			this.humidityBelowFloor = false;
			return;
		}

		if (this.humidityThresholdStartTime == 0L) {
			this.humidityThresholdStartTime = now;
			this.humidityAboveCeiling = humidity > this.triggerHumidifierCeiling;
			this.humidityBelowFloor = humidity < this.triggerHumidifierFloor;
			return;
		}

		long elapsedSec = (now - this.humidityThresholdStartTime) / 1000L;

		if (elapsedSec < this.humidityMaxTimePastThreshold) {
			return;
		}

		if (humidity > this.triggerHumidifierCeiling) {
			sendHumidifierActuationCommand(false);
			resetHumidityThresholdState();
		} else if (humidity < this.triggerHumidifierFloor) {
			sendHumidifierActuationCommand(true);
			resetHumidityThresholdState();
		}
	}

	private void handleHumiditySensorAnalysis(ResourceNameEnum resourceName, SensorData data)
	{
		float humidity = data.getValue();
		_Logger.info("Analyzing humidity data: " + humidity);

		boolean isLow = humidity < this.triggerHumidifierFloor;
		boolean isHigh = humidity > this.triggerHumidifierCeiling;
		boolean isNominal = !isLow && !isHigh;

		OffsetDateTime currentTimeStamp = getDateTimeFromData(data);

		if (isNominal) {
			this.latestHumiditySensorData = null;
			this.latestHumiditySensorTimeStamp = null;

			_Logger.info(
				"OFF check -> humidity=" + humidity +
				", nominal=" + this.nominalHumiditySetting +
				", lastKnown=" + this.lastKnownHumidifierCommand +
				", pending=" + this.pendingHumidifierCommand
			);

			if (this.lastKnownHumidifierCommand == ConfigConst.ON_COMMAND &&
				this.pendingHumidifierCommand != ConfigConst.OFF_COMMAND &&
				humidity >= this.nominalHumiditySetting) {

				ActuatorData offCommand = createHumidifierActuatorData(data, ConfigConst.OFF_COMMAND);

				_Logger.info("Humidity back to nominal. Sending OFF command.");

				if (sendActuatorCommandtoCda(ResourceNameEnum.CDA_ACTUATOR_CMD_RESOURCE, offCommand)) {
					this.pendingHumidifierCommand = ConfigConst.OFF_COMMAND;
				}
			}

			return;
		}

		if (this.latestHumiditySensorData == null) {
			this.latestHumiditySensorData = data;
			this.latestHumiditySensorTimeStamp = currentTimeStamp;

			_Logger.info(
				"Humidity moved outside nominal range. Starting threshold timer for " +
				this.humidityMaxTimePastThreshold + " seconds."
			);

			return;
		}

		float prevHumidity = this.latestHumiditySensorData.getValue();
		boolean prevWasLow = prevHumidity < this.triggerHumidifierFloor;
		boolean prevWasHigh = prevHumidity > this.triggerHumidifierCeiling;

		if ((isLow && !prevWasLow) || (isHigh && !prevWasHigh)) {
			this.latestHumiditySensorData = data;
			this.latestHumiditySensorTimeStamp = currentTimeStamp;

			_Logger.info("Humidity direction changed. Restarting threshold timer.");
			return;
		}

		long diffSeconds = java.time.temporal.ChronoUnit.SECONDS.between(
			this.latestHumiditySensorTimeStamp,
			currentTimeStamp
		);

		_Logger.info("Humidity threshold delta seconds: " + diffSeconds);

		if (diffSeconds < this.humidityMaxTimePastThreshold) {
			return;
		}

		int desiredCommand = isLow ? ConfigConst.ON_COMMAND : ConfigConst.OFF_COMMAND;

		if (desiredCommand == this.lastKnownHumidifierCommand ||
			desiredCommand == this.pendingHumidifierCommand) {
			_Logger.info("Desired humidifier command is already active or pending. Skipping duplicate command.");
			this.latestHumiditySensorData = null;
			this.latestHumiditySensorTimeStamp = null;
			return;
		}

		ActuatorData ad = createHumidifierActuatorData(data, desiredCommand);

		_Logger.info("Humidity threshold exceeded long enough. Sending actuator command: " + ad);

		if (sendActuatorCommandtoCda(ResourceNameEnum.CDA_ACTUATOR_CMD_RESOURCE, ad)) {
			this.pendingHumidifierCommand = desiredCommand;
		}

		this.latestHumiditySensorData = null;
		this.latestHumiditySensorTimeStamp = null;
	}

	private void resetHumidityThresholdState()
	{
		this.humidityThresholdStartTime = 0L;
		this.humidityAboveCeiling = false;
		this.humidityBelowFloor = false;
	}

	private boolean sendHumidifierActuationCommand(boolean enableHumidifier)
	{
		ActuatorData ad = new ActuatorData();

		ad.setName("Humidifier");
		ad.setValue(enableHumidifier ? 1.0f : 0.0f);

		this.lastHumidifierActuatorCommand = ad;

		_Logger.info("Sending humidifier actuator command: " + ad.getValue());

		String jsonData = DataUtil.getInstance().actuatorDataToJson(ad);

		if (this.mqttClient != null) {
			return this.mqttClient.publishMessage(
				ResourceNameEnum.CDA_ACTUATOR_CMD_RESOURCE,
				jsonData,
				ConfigConst.DEFAULT_QOS
			);
		}

		return false;
	}

	private boolean sendActuatorCommandtoCda(ResourceNameEnum resourceName, ActuatorData data)
	{
		if (this.actuatorDataListener != null) {
			this.actuatorDataListener.onActuatorDataUpdate(data);
		}

		if (this.enableMqttClient && this.mqttClient != null) {
			String jsonData = DataUtil.getInstance().actuatorDataToJson(data);

			if (this.mqttClient.publishMessage(resourceName, jsonData, ConfigConst.DEFAULT_QOS)) {
				_Logger.info("Published ActuatorData command from GDA to CDA: " + data.getCommand());
				return true;
			} else {
				_Logger.warning("Failed to publish ActuatorData command from GDA to CDA: " + data.getCommand());
			}
		}

		return false;
	}

	private boolean isHumidifierActuatorData(ActuatorData data)
	{
		return data != null &&
			(
				data.getTypeID() == ConfigConst.HUMIDIFIER_ACTUATOR_TYPE ||
				ConfigConst.HUMIDIFIER_ACTUATOR_NAME.equalsIgnoreCase(data.getName())
			);
	}

	private ActuatorData createHumidifierActuatorData(SensorData sensorData, int command)
	{
		ActuatorData ad = new ActuatorData();

		ad.setName(ConfigConst.HUMIDIFIER_ACTUATOR_NAME);
		ad.setLocationID(sensorData.getLocationID());
		ad.setTypeID(ConfigConst.HUMIDIFIER_ACTUATOR_TYPE);
		ad.setValue(this.nominalHumiditySetting);
		ad.setCommand(command);

		return ad;
	}

	private OffsetDateTime getDateTimeFromData(BaseIotData data)
	{
		try {
			return OffsetDateTime.parse(data.getTimeStamp());
		} catch (Exception e) {
			_Logger.warning("Failed to parse timestamp from data. Using current time.");
			return OffsetDateTime.now();
		}
	}

	@Override
	public void setActuatorDataListener(String name, IActuatorDataListener listener)
	{
		if (listener != null) {
			this.actuatorDataListener = listener;
		}
	}

	public void startManager()
	{
		_Logger.info("Starting DeviceDataManager...");

		if (this.sysPerfMgr != null) {
			this.sysPerfMgr.startManager();
		}

		if (this.mqttClient != null) {
			if (this.mqttClient.connectClient()) {
				_Logger.info("Successfully connected MQTT client to broker.");
			} else {
				_Logger.severe("Failed to connect MQTT client to broker.");
			}
		}

		if (this.cloudClient != null) {
			if (this.cloudClient.connectClient()) {
				_Logger.info("Successfully connected Cloud client.");
			} else {
				_Logger.warning("Failed to connect Cloud client.");
			}
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

		if (this.enableCoapServer && this.coapServer != null) {
			if (this.coapServer.startServer()) {
				_Logger.info("CoAP server started.");
			} else {
				_Logger.severe("Failed to start CoAP server. Check log file for details.");
			}
		}
	}

	public void stopManager()
	{
		_Logger.info("Stopping DeviceDataManager...");

		if (this.mqttClient != null) {
			this.mqttClient.unsubscribeFromTopic(ResourceNameEnum.GDA_MGMT_STATUS_MSG_RESOURCE);
			this.mqttClient.unsubscribeFromTopic(ResourceNameEnum.CDA_ACTUATOR_RESPONSE_RESOURCE);
			this.mqttClient.unsubscribeFromTopic(ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE);
			this.mqttClient.unsubscribeFromTopic(ResourceNameEnum.CDA_SYSTEM_PERF_MSG_RESOURCE);

			if (this.mqttClient.disconnectClient()) {
				_Logger.info("Successfully disconnected MQTT client from broker.");
			} else {
				_Logger.severe("Failed to disconnect MQTT client from broker.");
			}
		}

		if (this.cloudClient != null) {
			if (this.cloudClient.disconnectClient()) {
				_Logger.info("Successfully disconnected Cloud client.");
			} else {
				_Logger.warning("Failed to disconnect Cloud client.");
			}
		}

		if (this.sysPerfMgr != null) {
			this.sysPerfMgr.stopManager();
		}

		if (this.redisClient != null) {
			this.redisClient.disconnectClient();
		}

		if (this.enableCoapServer && this.coapServer != null) {
			if (this.coapServer.stopServer()) {
				_Logger.info("CoAP server stopped.");
			} else {
				_Logger.severe("Failed to stop CoAP server. Check log file for details.");
			}
		}
	}

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

	private void initConnections()
	{
	}

	private void initManager()
	{
		ConfigUtil configUtil = ConfigUtil.getInstance();

		this.enableSystemPerf =
			configUtil.getBoolean(
				ConfigConst.GATEWAY_DEVICE,
				ConfigConst.ENABLE_SYSTEM_PERF_KEY
			);

		if (this.enableSystemPerf) {
			this.sysPerfMgr = new SystemPerformanceManager();
			this.sysPerfMgr.setDataMessageListener(this);
		}

		if (this.enableMqttClient) {
			this.mqttClient = new MqttClientConnector();
			this.mqttClient.setDataMessageListener(this);
		}

		if (this.enableCoapServer) {
			this.coapServer = new CoapServerGateway(this);
		}

		if (this.enableCoapClient) {
			this.coapClient = new CoapClientConnector();
			this.coapClient.setDataMessageListener(this);
		}

		if (this.enableCloudClient) {
			this.cloudClient =
				new CloudClientConnector(ConfigConst.UBIDOTS_CLOUD_GATEWAY_SERVICE);
			this.cloudClient.setDataMessageListener(this);
		}

		if (this.enablePersistenceClient) {
			this.redisClient = new RedisPersistenceAdapter();
			_Logger.info("Redis Persistence client enabled.");
		}
	}
}
