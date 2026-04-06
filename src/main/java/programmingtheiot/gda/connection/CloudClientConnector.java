package programmingtheiot.gda.connection;

import java.lang.reflect.Method;
import java.util.logging.Logger;

import programmingtheiot.common.ConfigConst;
import programmingtheiot.common.ConfigUtil;
import programmingtheiot.common.IDataMessageListener;
import programmingtheiot.common.ResourceNameEnum;
import programmingtheiot.data.ActuatorData;
import programmingtheiot.data.DataUtil;
import programmingtheiot.data.SensorData;
import programmingtheiot.data.SystemPerformanceData;

public class CloudClientConnector implements ICloudClient, IConnectionListener
{
	private static final Logger _Logger =
		Logger.getLogger(CloudClientConnector.class.getName());

	private String cloudGatewaySectionName = ConfigConst.CLOUD_GATEWAY_SERVICE;
	private String topicPrefix = "";
	private MqttClientConnector mqttClient = null;
	private IDataMessageListener dataMsgListener = null;
	private int qosLevel = 1;

	public CloudClientConnector()
	{
		this(ConfigConst.CLOUD_GATEWAY_SERVICE);
	}

	public CloudClientConnector(String cloudGatewaySectionName)
	{
		ConfigUtil configUtil = ConfigUtil.getInstance();

		if (cloudGatewaySectionName != null &&
			cloudGatewaySectionName.trim().length() > 0) {
			this.cloudGatewaySectionName = cloudGatewaySectionName;
		}

		this.topicPrefix =
			configUtil.getProperty(
				this.cloudGatewaySectionName,
				ConfigConst.BASE_TOPIC_KEY);

		if (this.topicPrefix == null || this.topicPrefix.trim().length() == 0) {
			this.topicPrefix = "/";
		} else if (!this.topicPrefix.endsWith("/")) {
			this.topicPrefix += "/";
		}
	}

	@Override
	public boolean connectClient()
	{
		if (this.mqttClient == null) {
			this.mqttClient = new MqttClientConnector(this.cloudGatewaySectionName);
			this.mqttClient.setConnectionListener(this);
		}

		return this.mqttClient.connectClient();
	}

	@Override
	public boolean disconnectClient()
	{
		if (this.mqttClient != null && this.mqttClient.isConnected()) {
			return this.mqttClient.disconnectClient();
		}

		return false;
	}

	@Override
	public boolean sendEdgeDataToCloud(ResourceNameEnum resource, SensorData data)
	{
		if (resource != null && data != null) {
			String payload =
				DataUtil.getInstance().sensorDataToTimeAndValueJson(data);

			String itemName = mapSensorLabel(data);

			return publishMessageToCloud(resource, itemName, payload);
		}

		return false;
	}

	@Override
	public boolean sendEdgeDataToCloud(ResourceNameEnum resource, SystemPerformanceData data)
	{
		if (resource != null && data != null) {
			SensorData cpuData = new SensorData();
			cpuData.updateData(data);
			cpuData.setName("cpuutil");
			cpuData.setValue(
				getMetricValue(
					data,
					"getCpuUtilization",
					"getCpuUtil",
					"getCpuUtilPct",
					"getCpuUtilPercent"
				)
			);

			SensorData diskData = new SensorData();
			diskData.updateData(data);
			diskData.setName("diskutil");
			diskData.setValue(
				getMetricValue(
					data,
					"getDiskUtilization",
					"getDiskUtil",
					"getDiskUtilPct",
					"getDiskUtilPercent"
				)
			);

			SensorData memData = new SensorData();
			memData.updateData(data);
			memData.setName("memutil");
			memData.setValue(
				getMetricValue(
					data,
					"getMemoryUtilization",
					"getMemUtil",
					"getMemoryUtil",
					"getMemUtilPct",
					"getMemoryUtilPercent"
				)
			);

			boolean cpuOk = sendEdgeDataToCloud(resource, cpuData);
			boolean diskOk = sendEdgeDataToCloud(resource, diskData);
			boolean memOk = sendEdgeDataToCloud(resource, memData);

			return cpuOk && diskOk && memOk;
		}

		return false;
	}

	@Override
	public boolean sendEdgeDataToCloud(ResourceNameEnum resource, ActuatorData data)
	{
		if (resource != null && data != null) {
			SensorData stateData = new SensorData();
			stateData.updateData(data);                        
			stateData.setName(mapActuatorLabel(data));         // "humidifier" or "hvac"
			stateData.setValue((float) data.getCommand());     // ON=1, OFF=0

			String payload = DataUtil.getInstance().sensorDataToTimeAndValueJson(stateData);
			_Logger.info("Sending actuator state to cloud: " + stateData.getName() + "=" + stateData.getValue() + ", payload=" + payload);

			return publishMessageToCloud(resource, stateData.getName(), payload);
		}
		return false;
	}

	private String mapActuatorLabel(ActuatorData data)
	{
		if (data == null) return "actuator";

		String n = (data.getName() != null) ? data.getName().trim().toLowerCase() : "";

		if (n.contains("humid")) return "humidifier";
		if (n.contains("hvac"))  return "hvac";

		int typeID = data.getTypeID();
		if (typeID == ConfigConst.HUMIDIFIER_ACTUATOR_TYPE) return "humidifier";
		if (typeID == ConfigConst.HVAC_ACTUATOR_TYPE)       return "hvac";

		return n.isEmpty() || n.equals("not set") ? "actuator" : n;
	}

	@Override
	public boolean subscribeToCloudEvents(ResourceNameEnum resource)
	{
		boolean success = false;
		String topicName = null;

		if (this.mqttClient != null && this.mqttClient.isConnected()) {
			topicName = createTopicName(resource);
			this.mqttClient.subscribeToTopic(topicName, this.qosLevel);
			success = true;
		}

		return success;
	}

	@Override
	public boolean unsubscribeFromCloudEvents(ResourceNameEnum resource)
	{
		boolean success = false;
		String topicName = null;

		if (this.mqttClient != null && this.mqttClient.isConnected()) {
			topicName = createTopicName(resource);
			this.mqttClient.unsubscribeFromTopic(topicName);
			success = true;
		}

		return success;
	}

	@Override
	public boolean setDataMessageListener(IDataMessageListener listener)
	{
		if (listener != null) {
			this.dataMsgListener = listener;
			return true;
		}

		return false;
	}

	@Override
	public void onConnect()
	{
		_Logger.info("Cloud MQTT connection established.");
	}

	@Override
	public void onDisconnect()
	{
		_Logger.info("Cloud MQTT disconnected.");
	}

	private boolean publishMessageToCloud(ResourceNameEnum resource, String itemName, String payload)
	{
		String topicName = createTopicName(resource, itemName);
		return publishMessageToCloud(topicName, payload);
	}

	private boolean publishMessageToCloud(String topicName, String payload)
	{
		try {
			if (this.mqttClient != null && this.mqttClient.isConnected()) {
				return this.mqttClient.publishMessage(
					topicName,
					payload.getBytes(),
					this.qosLevel
				);
			}
		} catch (Exception e) {
			_Logger.warning("Failed to publish cloud message: " + e.getMessage());
		}

		return false;
	}

	private String createTopicName(ResourceNameEnum resource)
	{
		return createTopicName(resource.getDeviceName(), resource.getResourceType());
	}

	private String createTopicName(ResourceNameEnum resource, String itemName)
	{
		return (createTopicName(resource) + "-" + itemName).toLowerCase();
	}

	private String createTopicName(String deviceName, String resourceTypeName)
	{
		StringBuilder buf = new StringBuilder();

		if (deviceName != null && deviceName.trim().length() > 0) {
			buf.append(this.topicPrefix).append(deviceName);
		}

		if (resourceTypeName != null && resourceTypeName.trim().length() > 0) {
			buf.append('/').append(resourceTypeName);
		}

		return buf.toString().toLowerCase();
	}

	private String mapSensorLabel(SensorData data)
	{
		if (data == null || data.getName() == null) {
			return "sensor";
		}

		String normalized = data.getName().trim().toLowerCase();

		if (normalized.contains("temp")) {
			return "temp";
		} else if (normalized.contains("press")) {
			return "pressure";
		} else if (normalized.contains("humid")) {
			return "humidity";
		}

		return normalized;
	}

	private float getMetricValue(Object data, String... methodNames)
	{
		if (data == null || methodNames == null) {
			return 0.0f;
		}

		for (String methodName : methodNames) {
			try {
				Method method = data.getClass().getMethod(methodName);
				Object result = method.invoke(data);

				if (result instanceof Number) {
					return ((Number) result).floatValue();
				}
			} catch (Exception e) {
				// ignore and try next candidate
			}
		}

		return 0.0f;
	}
}