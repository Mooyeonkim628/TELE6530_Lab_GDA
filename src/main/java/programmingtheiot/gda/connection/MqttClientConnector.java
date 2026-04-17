package programmingtheiot.gda.connection;

import java.io.File;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.SSLSocketFactory;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import programmingtheiot.common.ConfigConst;
import programmingtheiot.common.ConfigUtil;
import programmingtheiot.common.IDataMessageListener;
import programmingtheiot.common.ResourceNameEnum;
import programmingtheiot.common.SimpleCertManagementUtil;
import programmingtheiot.data.ActuatorData;
import programmingtheiot.data.DataUtil;
import programmingtheiot.data.SensorData;
import programmingtheiot.data.SystemPerformanceData;

public class MqttClientConnector implements IPubSubClient, MqttCallbackExtended
{
	private static final Logger _Logger =
		Logger.getLogger(MqttClientConnector.class.getName());

	private boolean useAsyncClient = true;
	private MqttAsyncClient mqttClient = null;
	private MqttConnectOptions connOpts = null;
	private MemoryPersistence persistence = null;
	private IDataMessageListener dataMsgListener = null;

	private String clientID = null;
	private String brokerAddr = null;
	private String host = ConfigConst.DEFAULT_HOST;
	private String protocol = ConfigConst.DEFAULT_MQTT_PROTOCOL;
	private int port = ConfigConst.DEFAULT_MQTT_PORT;
	private int brokerKeepAlive = ConfigConst.DEFAULT_KEEP_ALIVE;

	private int securePort = 8883;
	private boolean enableCrypt = false;
	private String certFile = null;
	private boolean useCleanSession = false;
	private boolean enableAutoReconnect = true;

	private IConnectionListener connListener = null;
	private boolean useCloudGatewayConfig = false;

	public MqttClientConnector()
	{
		this(false);
	}

	public MqttClientConnector(boolean useCloudGatewayConfig)
	{
		this(useCloudGatewayConfig ? ConfigConst.CLOUD_GATEWAY_SERVICE : null);
	}

	public MqttClientConnector(String cloudGatewayConfigSectionName)
	{
		super();

		if (cloudGatewayConfigSectionName != null &&
			cloudGatewayConfigSectionName.trim().length() > 0) {
			this.useCloudGatewayConfig = true;
			initClientParameters(cloudGatewayConfigSectionName);
		} else {
			this.useCloudGatewayConfig = false;
			initClientParameters(ConfigConst.MQTT_GATEWAY_SERVICE);
		}
	}

	public boolean connectClient()
	{
		try {
			if (this.mqttClient == null) {
				this.mqttClient = new MqttAsyncClient(this.brokerAddr, this.clientID);
				this.mqttClient.setCallback(this);
			}

			IMqttToken token = this.mqttClient.connect(this.connOpts);
			token.waitForCompletion();

			return true;
		} catch (Exception e) {
			_Logger.warning("Failed to connect MQTT client: " + e.getMessage());
		}

		return false;
	}

	public boolean disconnectClient()
	{
		try {
			if (this.mqttClient != null && this.mqttClient.isConnected()) {
				IMqttToken token = this.mqttClient.disconnect();
				token.waitForCompletion();
			}

			return true;
		} catch (Exception e) {
			_Logger.warning("Failed to disconnect MQTT client: " + e.getMessage());
		}

		return false;
	}

	public boolean isConnected()
	{
		return (this.mqttClient != null && this.mqttClient.isConnected());
	}

	protected boolean publishMessage(String topicName, byte[] payload, int qos)
	{
		if (topicName == null) {
			_Logger.warning("Resource is null. Unable to publish message: " + this.brokerAddr);
			return false;
		}

		if (payload == null || payload.length == 0) {
			_Logger.warning("Message is null or empty. Unable to publish message: " + this.brokerAddr);
			return false;
		}

		if (qos < 0 || qos > 2) {
			_Logger.warning("Invalid QoS. Using default. QoS requested: " + qos);
			qos = ConfigConst.DEFAULT_QOS;
		}

		try {
			MqttMessage mqttMsg = new MqttMessage();
			mqttMsg.setQos(qos);
			mqttMsg.setPayload(payload);

			this.mqttClient.publish(topicName, mqttMsg);

			return true;
		} catch (Exception e) {
			_Logger.log(Level.SEVERE, "Failed to publish message to topic: " + topicName, e);
		}

		return false;
	}

	@Override
	public boolean publishMessage(ResourceNameEnum topicName, String msg, int qos)
	{
		if (topicName == null) {
			_Logger.warning("Resource is null. Unable to publish message: " + this.brokerAddr);
			return false;
		}

		if (msg == null || msg.length() == 0) {
			_Logger.warning("Message is null or empty. Unable to publish message: " + this.brokerAddr);
			return false;
		}

		return publishMessage(topicName.getResourceName(), msg.getBytes(), qos);
	}

	protected boolean subscribeToTopic(String topicName, int qos)
	{
		return subscribeToTopic(topicName, qos, null);
	}

	protected boolean subscribeToTopic(
		String topicName,
		int qos,
		org.eclipse.paho.client.mqttv3.IMqttMessageListener listener)
	{
		if (topicName == null) {
			_Logger.warning("Resource is null. Unable to subscribe to topic: " + this.brokerAddr);
			return false;
		}

		if (qos < 0 || qos > 2) {
			_Logger.warning("Invalid QoS. Using default. QoS requested: " + qos);
			qos = ConfigConst.DEFAULT_QOS;
		}

		try {
			if (listener != null) {
				this.mqttClient.subscribe(topicName, qos, listener);
				_Logger.info("Successfully subscribed to topic with listener: " + topicName);
			} else {
				this.mqttClient.subscribe(topicName, qos);
				_Logger.info("Successfully subscribed to topic: " + topicName);
			}

			return true;
		} catch (Exception e) {
			_Logger.log(Level.SEVERE, "Failed to subscribe to topic: " + topicName, e);
		}

		return false;
	}

	@Override
	public boolean subscribeToTopic(ResourceNameEnum topicName, int qos)
	{
		if (topicName == null) {
			_Logger.warning("Resource is null. Unable to subscribe to topic: " + this.brokerAddr);
			return false;
		}

		return subscribeToTopic(topicName.getResourceName(), qos);
	}

	protected boolean unsubscribeFromTopic(String topicName)
	{
		if (topicName == null) {
			_Logger.warning("Resource is null. Unable to unsubscribe from topic: " + this.brokerAddr);
			return false;
		}

		try {
			this.mqttClient.unsubscribe(topicName);
			_Logger.info("Successfully unsubscribed from topic: " + topicName);
			return true;
		} catch (Exception e) {
			_Logger.log(Level.SEVERE, "Failed to unsubscribe from topic: " + topicName, e);
		}

		return false;
	}

	@Override
	public boolean unsubscribeFromTopic(ResourceNameEnum topicName)
	{
		if (topicName == null) {
			_Logger.warning("Resource is null. Unable to unsubscribe from topic: " + this.brokerAddr);
			return false;
		}

		return unsubscribeFromTopic(topicName.getResourceName());
	}

	@Override
	public boolean setConnectionListener(IConnectionListener listener)
	{
		if (listener != null) {
			_Logger.info("Setting connection listener.");
			this.connListener = listener;
			return true;
		} else {
			_Logger.warning("No connection listener specified. Ignoring.");
		}

		return false;
	}

	public boolean setDataMessageListener(IDataMessageListener listener)
	{
		if (listener != null) {
			this.dataMsgListener = listener;
			return true;
		}

		return false;
	}

	@Override
	public void connectComplete(boolean reconnect, String serverURI)
	{
		_Logger.info("MQTT connection successful (is reconnect = " + reconnect + "). Broker: " + serverURI);

		int qos = 1;

		if (!this.useCloudGatewayConfig) {
			this.subscribeToTopic(ResourceNameEnum.CDA_ACTUATOR_RESPONSE_RESOURCE, qos);
			this.subscribeToTopic(ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE, qos);
		}

		if (this.connListener != null) {
			this.connListener.onConnect();
		}
	}

	@Override
	public void connectionLost(Throwable t)
	{
		_Logger.log(Level.WARNING, "Lost connection to MQTT broker: " + this.brokerAddr, t);
	}

	@Override
	public void deliveryComplete(IMqttDeliveryToken token)
	{
		// no-op
	}

	@Override
	public void messageArrived(String topic, MqttMessage message) throws Exception
	{
		_Logger.info("MQTT message arrived on topic: '" + topic + "'");

		if (message == null) {
			_Logger.warning("MQTT message is null.");
			return;
		}

		try {
			String payload = new String(message.getPayload());

			if (topic.equals(ResourceNameEnum.CDA_ACTUATOR_RESPONSE_RESOURCE.getResourceName())) {
				ActuatorData actuatorData = DataUtil.getInstance().jsonToActuatorData(payload);

				if (actuatorData != null) {
					_Logger.info("Received ActuatorData response: " + actuatorData.getValue());

					if (this.dataMsgListener != null) {
						this.dataMsgListener.handleActuatorCommandResponse(
							ResourceNameEnum.CDA_ACTUATOR_RESPONSE_RESOURCE,
							actuatorData
						);
					} else {
						handleActuatorCommandResponse(
							ResourceNameEnum.CDA_ACTUATOR_RESPONSE_RESOURCE,
							actuatorData
						);
					}
				}
			}
			else if (topic.equals(ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE.getResourceName())) {
				SensorData sensorData = DataUtil.getInstance().jsonToSensorData(payload);

				if (sensorData != null) {
					_Logger.info("Received SensorData: " + sensorData);

					if (this.dataMsgListener != null) {
						this.dataMsgListener.handleSensorMessage(
							ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE,
							sensorData
						);
					} else {
						handleSensorMessage(
							ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE,
							sensorData
						);
					}
				}
			}
			else if (topic.equals(ResourceNameEnum.CDA_SYSTEM_PERF_MSG_RESOURCE.getResourceName())) {
				_Logger.info("Ignoring CDA system performance message.");
				return;
			}
			else {
				_Logger.info("Received cloud control message on topic: " + topic);

				if (this.dataMsgListener != null) {
					try {
						// Ubidots /lv topic sends plain float (e.g. "1.0") or JSON
						String clean = payload.trim();
						float value = 0.0f;
						try {
							value = Float.parseFloat(clean);
						} catch (NumberFormatException e) {
							if (clean.startsWith("[")) {
								clean = clean.substring(1, clean.lastIndexOf("]")).trim();
							}
							java.util.regex.Matcher m =
								java.util.regex.Pattern.compile("\"value\"\\s*:\\s*([0-9.]+)").matcher(clean);
							if (m.find()) value = Float.parseFloat(m.group(1));
						}

						ActuatorData ad = new ActuatorData();
						if (topic.contains("hvac")) {
							ad.setName(ConfigConst.HVAC_ACTUATOR_NAME);
							ad.setTypeID(ConfigConst.HVAC_ACTUATOR_TYPE);
						} else if (topic.contains("humidifier")) {
							ad.setName(ConfigConst.HUMIDIFIER_ACTUATOR_NAME);
							ad.setTypeID(ConfigConst.HUMIDIFIER_ACTUATOR_TYPE);
						}
						ad.setCommand((int) value);
						ad.setValue(value);

						// CDA location ID 설정 - location mismatch 방지
						ad.setLocationID(
							ConfigUtil.getInstance().getProperty(
								ConfigConst.CONSTRAINED_DEVICE,
								ConfigConst.DEVICE_LOCATION_ID_KEY,
								"CDA_Mooyeon_Kim"
							)
						);

						String jsonData = DataUtil.getInstance().actuatorDataToJson(ad);
						_Logger.info("Cloud command parsed: " + jsonData);

						this.dataMsgListener.handleIncomingMessage(
							ResourceNameEnum.CDA_ACTUATOR_CMD_RESOURCE, jsonData);

					} catch (Exception e) {
						_Logger.warning("Failed to process cloud control message: " + e.getMessage());
					}
				}
			}
		}
		catch (Exception e) {
			_Logger.log(Level.SEVERE, "Failed to process incoming MQTT message on topic: " + topic, e);
		}
	}

	private void initClientParameters(String configSectionName)
	{
		ConfigUtil configUtil = ConfigUtil.getInstance();

		this.host =
			configUtil.getProperty(
				configSectionName, ConfigConst.HOST_KEY, ConfigConst.DEFAULT_HOST);
		this.port =
			configUtil.getInteger(
				configSectionName, ConfigConst.PORT_KEY, ConfigConst.DEFAULT_MQTT_PORT);
		this.brokerKeepAlive =
			configUtil.getInteger(
				configSectionName, ConfigConst.KEEP_ALIVE_KEY, ConfigConst.DEFAULT_KEEP_ALIVE);
		this.enableCrypt =
			configUtil.getBoolean(
				configSectionName, ConfigConst.ENABLE_CRYPT_KEY);
		this.certFile =
			configUtil.getProperty(
				configSectionName, ConfigConst.CERT_FILE_KEY);

		this.useAsyncClient =
			configUtil.getBoolean(
				configSectionName, ConfigConst.USE_ASYNC_CLIENT_KEY);

		this.clientID =
			configUtil.getProperty(
				ConfigConst.GATEWAY_DEVICE,
				ConfigConst.DEVICE_LOCATION_ID_KEY,
				MqttClient.generateClientId()
			);

		this.persistence = new MemoryPersistence();
		this.connOpts = new MqttConnectOptions();

		this.connOpts.setKeepAliveInterval(this.brokerKeepAlive);
		this.connOpts.setCleanSession(this.useCleanSession);
		this.connOpts.setAutomaticReconnect(this.enableAutoReconnect);

		if (this.enableCrypt) {
			initSecureConnectionParameters(configSectionName);
		}

		if (configUtil.hasProperty(configSectionName, ConfigConst.CRED_FILE_KEY)) {
			initCredentialConnectionParameters(configSectionName);
		}

		this.brokerAddr = this.protocol + "://" + this.host + ":" + this.port;

		_Logger.info("Using URL for broker conn: " + this.brokerAddr);
	}

	private void initCredentialConnectionParameters(String configSectionName)
	{
		ConfigUtil configUtil = ConfigUtil.getInstance();

		try {
			_Logger.info("Checking if credentials file exists and is loadable...");

			Properties props = configUtil.getCredentials(configSectionName);

			if (props != null) {
				this.connOpts.setUserName(props.getProperty(ConfigConst.USER_NAME_TOKEN_KEY, ""));
				this.connOpts.setPassword(props.getProperty(ConfigConst.USER_AUTH_TOKEN_KEY, "").toCharArray());

				_Logger.info("Credentials now set.");
			} else {
				_Logger.warning("No credentials are set.");
			}
		} catch (Exception e) {
			_Logger.log(Level.WARNING, "Credential file non-existent. Disabling auth requirement.");
		}
	}

	private void initSecureConnectionParameters(String configSectionName)
	{
		ConfigUtil configUtil = ConfigUtil.getInstance();

		try {
			_Logger.info("Configuring TLS...");

			if (this.certFile != null) {
				File file = new File(this.certFile);

				if (file.exists()) {
					_Logger.info("PEM file valid. Using secure connection: " + this.certFile);
				} else {
					this.enableCrypt = false;

					_Logger.log(
						Level.WARNING,
						"PEM file invalid. Using insecure connection: " + this.certFile,
						new Exception()
					);

					return;
				}
			}

			SSLSocketFactory sslFactory =
				SimpleCertManagementUtil.getInstance().loadCertificate(this.certFile);

			this.connOpts.setSocketFactory(sslFactory);

			this.port =
				configUtil.getInteger(
					configSectionName, ConfigConst.SECURE_PORT_KEY, ConfigConst.DEFAULT_MQTT_SECURE_PORT);

			this.protocol = ConfigConst.DEFAULT_MQTT_SECURE_PROTOCOL;

			_Logger.info("TLS enabled.");
		} catch (Exception e) {
			_Logger.log(
				Level.SEVERE,
				"Failed to initialize secure MQTT connection. Using insecure connection.",
				e
			);

			this.enableCrypt = false;
		}
	}

	protected boolean handleActuatorCommandResponse(ResourceNameEnum resourceName, ActuatorData data)
	{
		if (data == null) {
			_Logger.warning("ActuatorData is null.");
			return false;
		}

		_Logger.info("Processing ActuatorData internally for resource: " + resourceName.getResourceName());
		_Logger.info("Actuator response value: " + data.getValue());

		return true;
	}

	protected boolean handleSensorMessage(ResourceNameEnum resourceName, SensorData data)
	{
		if (data == null) {
			_Logger.warning("SensorData is null.");
			return false;
		}

		_Logger.info("Processing SensorData internally for resource: " + resourceName.getResourceName());
		_Logger.info("SensorData value: " + data.getValue());

		return true;
	}

	protected boolean handleSystemPerformanceMessage(ResourceNameEnum resourceName, SystemPerformanceData data)
	{
		if (data == null) {
			_Logger.warning("SystemPerformanceData is null.");
			return false;
		}

		_Logger.info("Processing SystemPerformanceData internally for resource: " + resourceName.getResourceName());
		return true;
	}
}