package programmingtheiot.integration.connection;

import java.util.logging.Logger;

import org.junit.After;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

import programmingtheiot.common.ConfigConst;
import programmingtheiot.common.ResourceNameEnum;
import programmingtheiot.data.SensorData;
import programmingtheiot.gda.app.DeviceDataManager;
import programmingtheiot.gda.connection.CloudClientFactory;
import programmingtheiot.gda.connection.ICloudClient;

public class CloudClientConnectorTest
{
	private static final Logger _Logger =
		Logger.getLogger(CloudClientConnectorTest.class.getName());

	private ICloudClient cloudClient = null;

	@Before
	public void setUp() throws Exception
	{
		this.cloudClient = CloudClientFactory.getInstance().createCloudClient();

		assertNotNull(
			"Cloud client should not be null after factory creation.",
			this.cloudClient
		);
	}

	@After
	public void tearDown() throws Exception
	{
		if (this.cloudClient != null) {
			try {
				this.cloudClient.disconnectClient();
			} catch (Exception e) {
				_Logger.warning("Error during cloud client disconnect: " + e.getMessage());
			}
		}
	}

	@Test
	public void testIntegratedConnectDisconnect()
	{
		boolean connected = this.cloudClient.connectClient();

		assertTrue(
			"Cloud client should connect successfully.",
			connected
		);

		boolean disconnected = this.cloudClient.disconnectClient();

		assertTrue(
			"Cloud client should disconnect successfully.",
			disconnected
		);
	}

	@Test
	public void testCloudClientPublishSensorDataToCloud()
	{
		boolean connected = this.cloudClient.connectClient();

		assertTrue(
			"Cloud client should connect successfully before publish.",
			connected
		);

		SensorData sensorData = new SensorData();
		sensorData.setName("TestTemperatureSensor");
		sensorData.setTypeID(ConfigConst.TEMP_SENSOR_TYPE);
		sensorData.setLocationID("GDA_TEST_DEVICE");
		sensorData.setValue(23.5f);

		boolean published =
			this.cloudClient.sendEdgeDataToCloud(
				ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE,
				sensorData
			);

		assertTrue(
			"Cloud client should publish SensorData successfully.",
			published
		);
	}

	@Test
	public void testCloudClientThresholdCrossingAndActuationEvent() throws Exception
	{
		DeviceDataManager deviceDataManager = new DeviceDataManager();

		deviceDataManager.startManager();

		SensorData humidityData1 = new SensorData();
		humidityData1.setName("TestHumiditySensor");
		humidityData1.setTypeID(ConfigConst.HUMIDITY_SENSOR_TYPE);
		humidityData1.setLocationID("GDA_TEST_DEVICE");
		humidityData1.setValue(20.0f);

		boolean handled1 =
			deviceDataManager.handleSensorMessage(
				ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE,
				humidityData1
			);

		assertTrue(
			"DeviceDataManager should handle first humidity sensor message.",
			handled1
		);

		Thread.sleep(6000);

		SensorData humidityData2 = new SensorData();
		humidityData2.setName("TestHumiditySensor");
		humidityData2.setTypeID(ConfigConst.HUMIDITY_SENSOR_TYPE);
		humidityData2.setLocationID("GDA_TEST_DEVICE");
		humidityData2.setValue(20.0f);

		boolean handled2 =
			deviceDataManager.handleSensorMessage(
				ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE,
				humidityData2
			);

		assertTrue(
			"DeviceDataManager should handle second humidity sensor message.",
			handled2
		);

		deviceDataManager.stopManager();
	}
}