package programmingtheiot.integration.app;

import static org.junit.Assert.assertNotNull;
import org.junit.Test;

import programmingtheiot.common.ConfigConst;
import programmingtheiot.common.ConfigUtil;
import programmingtheiot.common.ResourceNameEnum;
import programmingtheiot.data.SensorData;
import programmingtheiot.gda.app.DeviceDataManager;

public class DeviceDataManagerSimpleCdaActuationTest
{
	@Test
	public void testSendActuationEventsToCda()
	{
		DeviceDataManager devDataMgr = new DeviceDataManager();
		assertNotNull(devDataMgr);

		devDataMgr.startManager();

		ConfigUtil cfgUtil = ConfigUtil.getInstance();

		float nominalVal = cfgUtil.getFloat(
			ConfigConst.GATEWAY_DEVICE,
			"nominalHumiditySetting");

		float lowVal = cfgUtil.getFloat(
			ConfigConst.GATEWAY_DEVICE,
			"triggerHumidifierFloor");

		float highVal = cfgUtil.getFloat(
			ConfigConst.GATEWAY_DEVICE,
			"triggerHumidifierCeiling");

		int delay = cfgUtil.getInteger(
			ConfigConst.GATEWAY_DEVICE,
			"humidityMaxTimePastThreshold");

		generateAndProcessHumiditySensorDataSequence(
			devDataMgr, nominalVal, lowVal, highVal, delay);

		devDataMgr.stopManager();
	}

	private void generateAndProcessHumiditySensorDataSequence(
		DeviceDataManager ddm, float nominalVal, float lowVal, float highVal, int delay)
	{
		SensorData sd = new SensorData();
		sd.setName("My Test Humidity Sensor");
		sd.setLocationID("constraineddevice001");
		sd.setTypeID(ConfigConst.HUMIDITY_SENSOR_TYPE);

		sd.setValue(nominalVal);
		ddm.handleSensorMessage(ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE, sd);
		waitForSeconds(2);

		sd.setValue(nominalVal);
		ddm.handleSensorMessage(ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE, sd);
		waitForSeconds(2);

		sd.setValue(lowVal - 2);
		ddm.handleSensorMessage(ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE, sd);
		waitForSeconds(delay + 1);

		sd.setValue(lowVal - 1);
		ddm.handleSensorMessage(ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE, sd);
		waitForSeconds(delay + 1);

		sd.setValue(nominalVal);
		ddm.handleSensorMessage(ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE, sd);
		waitForSeconds(delay + 1);

		sd.setValue(nominalVal);
		ddm.handleSensorMessage(ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE, sd);
		waitForSeconds(delay + 1);

		sd.setValue(highVal + 2);
		ddm.handleSensorMessage(ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE, sd);
		waitForSeconds(delay + 1);

		sd.setValue(highVal + 1);
		ddm.handleSensorMessage(ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE, sd);
		waitForSeconds(delay + 1);

		sd.setValue(nominalVal);
		ddm.handleSensorMessage(ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE, sd);
		waitForSeconds(delay + 1);

		sd.setValue(nominalVal);
		ddm.handleSensorMessage(ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE, sd);
		waitForSeconds(delay + 1);
	}

	private void waitForSeconds(int seconds)
	{
		try {
			Thread.sleep(seconds * 1000L);
		} catch (InterruptedException e) {
			// ignore
		}
	}
}