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

//import programmingtheiot.common.DataUtil;
/**
 * Shell representation of class for student implementation.
 * 
 */
public class MqttClientConnector implements IPubSubClient, MqttCallbackExtended
{
	// static
	
	private static final Logger _Logger =
		Logger.getLogger(MqttClientConnector.class.getName());
	
	// params
	private boolean useAsyncClient = true;
	private MqttAsyncClient mqttClient = null;
	//private MqttClient           mqttClient = null;
	private MqttConnectOptions   connOpts = null;
	private MemoryPersistence    persistence = null;
	private IDataMessageListener dataMsgListener = null;

	private String  clientID = null;
	private String  brokerAddr = null;
	private String  host = ConfigConst.DEFAULT_HOST;
	private String  protocol = ConfigConst.DEFAULT_MQTT_PROTOCOL;
	private int     port = ConfigConst.DEFAULT_MQTT_PORT;
	private int     brokerKeepAlive = ConfigConst.DEFAULT_KEEP_ALIVE;
	
	private int     securePort = 8883;
	private boolean enableCrypt = false;
	private String  certFile = null;
	private boolean useCleanSession = false;
	private boolean enableAutoReconnect = true;
	// constructors
	
	/**
	 * Default.
	 * 
	 */
	public MqttClientConnector()
	{
		super();
		
		initClientParameters(ConfigConst.MQTT_GATEWAY_SERVICE);
	}
	
	
	// public methods
	
	public boolean connectClient()
	{
		if (this.mqttClient != null && this.mqttClient.isConnected()) {
			return true;
		}

		try {
			if (this.mqttClient == null) {
				this.mqttClient = new MqttAsyncClient(this.brokerAddr, this.clientID);
				this.mqttClient.setCallback(this);
			}

			_Logger.info("MQTT client connecting to broker: " + this.brokerAddr);

			IMqttToken token = this.mqttClient.connect(this.connOpts);
			token.waitForCompletion();

			return this.mqttClient.isConnected();
		}
		catch (Exception e) {
			_Logger.log(Level.WARNING, "Failed to connect MQTT client to broker: " + this.brokerAddr, e);
			return false;
		}
	}

	public boolean disconnectClient()
	{
		if (this.mqttClient == null || !this.mqttClient.isConnected()) {
			return true;
		}

		try {
			_Logger.info("Disconnecting MQTT client from broker: " + this.brokerAddr);

			IMqttToken token = this.mqttClient.disconnect();
			token.waitForCompletion();

			return true;
		}
		catch (Exception e) {
			_Logger.log(Level.WARNING, "Failed to disconnect MQTT client from broker.", e);
			return false;
		}
	}

	public boolean isConnected()
	{
		return (this.mqttClient != null && this.mqttClient.isConnected());
	}
	
	public boolean publishMessage(ResourceNameEnum resource, String msg, int qos)
	{
		if (this.mqttClient == null || !this.mqttClient.isConnected()) {
			_Logger.warning("MQTT client is null or not connected. Can't publish message.");
			return false;
		}

		if (resource == null || msg == null) {
			_Logger.warning("Resource or message is null. Can't publish message.");
			return false;
		}

		try {
			String topic = resource.getResourceName();

			_Logger.info("Publishing MQTT message to topic: " + topic);

			MqttMessage mqttMessage = new MqttMessage(msg.getBytes());
			mqttMessage.setQos(qos);

			this.mqttClient.publish(topic, mqttMessage);

			return true;
		}
		catch (Exception e) {
			_Logger.log(Level.WARNING, "Failed to publish MQTT message.", e);
			return false;
		}
	}

	public boolean subscribeToTopic(ResourceNameEnum resource, int qos)
	{
		if (this.mqttClient == null || !this.mqttClient.isConnected()) {
			_Logger.warning("MQTT client is null or not connected. Can't subscribe to topic.");
			return false;
		}

		try {
			String topic = resource.getResourceName();

			_Logger.info("Subscribing to topic: " + topic);
			this.mqttClient.subscribe(topic, qos);

			return true;
		}
		catch (Exception e) {
			_Logger.log(Level.WARNING, "Failed to subscribe to topic: " + resource.getResourceName(), e);
			return false;
		}
	}

	@Override
	public boolean unsubscribeFromTopic(ResourceNameEnum topicName)
	{
		if (topicName == null) {
			_Logger.warning("Resource is null. Unable to unsubscribe from topic: " + this.brokerAddr);
			return false;
		}
		
		try {
			this.mqttClient.unsubscribe(topicName.getResourceName());
			_Logger.info("Successfully unsubscribed from topic: " + topicName.getResourceName());
			return true;
		} catch (Exception e) {
			_Logger.log(Level.SEVERE, "Failed to unsubscribe from topic: " + topicName, e);
		}
		
		return false;
	}

	@Override
	public boolean setConnectionListener(IConnectionListener listener)
	{
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
	
	// callbacks
	
	@Override
	public void connectComplete(boolean reconnect, String serverURI)
	{
		_Logger.info("MQTT connection successful (is reconnect = " + reconnect + "). Broker: " + serverURI);

		subscribeToTopic(ResourceNameEnum.CDA_ACTUATOR_RESPONSE_RESOURCE, ConfigConst.DEFAULT_QOS);
		subscribeToTopic(ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE, ConfigConst.DEFAULT_QOS);
		subscribeToTopic(ResourceNameEnum.CDA_SYSTEM_PERF_MSG_RESOURCE, ConfigConst.DEFAULT_QOS);
	}

	public void connectionLost(Throwable t)
	{
		_Logger.log(Level.WARNING, "Lost connection to MQTT broker: " + this.brokerAddr, t);
	}
	
	public void deliveryComplete(IMqttDeliveryToken token)
	{
		//Logger.info("Delivered MQTT message with ID: " + token.getMessageId());
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
				SystemPerformanceData sysPerfData =
					DataUtil.getInstance().jsonToSystemPerformanceData(payload);

				if (sysPerfData != null) {
					_Logger.info("Received SystemPerformanceData: " + sysPerfData);

					if (this.dataMsgListener != null) {
						this.dataMsgListener.handleSystemPerformanceMessage(
							ResourceNameEnum.CDA_SYSTEM_PERF_MSG_RESOURCE,
							sysPerfData
						);
					} else {
						handleSystemPerformanceMessage(
							ResourceNameEnum.CDA_SYSTEM_PERF_MSG_RESOURCE,
							sysPerfData
						);
					}
				}
			}
			else {
				_Logger.warning("Unhandled MQTT topic: " + topic);
			}
		}
		catch (Exception e) {
			_Logger.log(Level.SEVERE, "Failed to process incoming MQTT message on topic: " + topic, e);
		}
	}
	
	// private methods
	
	/**
	 * Called by the constructor to set the MQTT client parameters to be used for the connection.
	 * 
	 * @param configSectionName The name of the configuration section to use for
	 * the MQTT client configuration parameters.
	 */
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
		this.enableCrypt  =
			configUtil.getBoolean(
				configSectionName, ConfigConst.ENABLE_CRYPT_KEY);
		this.certFile =
			configUtil.getProperty(
				configSectionName, ConfigConst.CERT_FILE_KEY);
		
		// This next config file boolean property is optional; it can be
		// set within the [Mqtt.GatewayService] and [Cloud.GatewayService]
		// sections of PiotConfig.props. You can use it to create a logical
		// flow within this class to determine whether to use MqttClient
		// or MqttAsyncClient, or simply choose one of the two classes based
		// on your usage needs. Generally speaking, MqttAsyncClient will
		// be necessary when running the GDA as an application, as it will
		// need to handle incoming and outgoing messages using MQTT
		// simultaneously. For GDA-only testing using the test cases
		// specified in this lab module and others, it's generally best -
		// and likely required - to use MqttClient.
		// 
		// IMPORTANT: If you're using an older version of ConfigConst.java,
		// you'll need to add the following line of code to ConfigConst.java:
		// public static final String USE_ASYNC_CLIENT_KEY = "useAsyncClient";
		this.useAsyncClient =
			configUtil.getBoolean(
				ConfigConst.MQTT_GATEWAY_SERVICE, ConfigConst.USE_ASYNC_CLIENT_KEY);

		// NOTE: updated from Lab Module 07 - attempt to load clientID from configuration file
		this.clientID =
			configUtil.getProperty(
				ConfigConst.GATEWAY_DEVICE, ConfigConst.DEVICE_LOCATION_ID_KEY, MqttClient.generateClientId());
		
		// these are specific to the MQTT connection which will be used during connect
		this.persistence = new MemoryPersistence();
		this.connOpts    = new MqttConnectOptions();
		
		this.connOpts.setKeepAliveInterval(this.brokerKeepAlive);
		this.connOpts.setCleanSession(this.useCleanSession);
		this.connOpts.setAutomaticReconnect(this.enableAutoReconnect);
		
		// if encryption is enabled, try to load and apply the cert(s)
		if (this.enableCrypt) {
			initSecureConnectionParameters(configSectionName);
		}
		
		// if there's a credential file, try to load and apply them
		if (configUtil.hasProperty(configSectionName, ConfigConst.CRED_FILE_KEY)) {
			initCredentialConnectionParameters(configSectionName);
		}
		
		// NOTE: URL does not have a protocol handler for "tcp" or "ssl",
		// so construct the URL manually
		this.brokerAddr  = this.protocol + "://" + this.host + ":" + this.port;
		
		_Logger.info("Using URL for broker conn: " + this.brokerAddr);
	}
	/**
	 * Called by {@link #initClientParameters(String)} to load credentials.
	 * 
	 * @param configSectionName The name of the configuration section to use for
	 * the MQTT client configuration parameters.
	 */
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
	
	/**
	 * Called by {@link #initClientParameters(String)} to enable encryption.
	 * 
	 * @param configSectionName The name of the configuration section to use for
	 * the MQTT client configuration parameters.
	 */
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
					
					_Logger.log(Level.WARNING, "PEM file invalid. Using insecure connection: " + this.certFile, new Exception());
					
					return;
				}
			}
			
			SSLSocketFactory sslFactory =
				SimpleCertManagementUtil.getInstance().loadCertificate(this.certFile);
			
			this.connOpts.setSocketFactory(sslFactory);
			
			// override current config parameters
			this.port =
				configUtil.getInteger(
					configSectionName, ConfigConst.SECURE_PORT_KEY, ConfigConst.DEFAULT_MQTT_SECURE_PORT);
			
			this.protocol = ConfigConst.DEFAULT_MQTT_SECURE_PROTOCOL;
			
			_Logger.info("TLS enabled.");
		} catch (Exception e) {
			_Logger.log(Level.SEVERE, "Failed to initialize secure MQTT connection. Using insecure connection.", e);
			
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
