package programmingtheiot.integration.connection;

import java.util.logging.Logger;

import org.junit.After;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

import programmingtheiot.common.ConfigConst;
import programmingtheiot.common.ConfigUtil;
import programmingtheiot.common.ResourceNameEnum;
import programmingtheiot.gda.connection.MqttClientConnector;

/**
 * This test case class contains very basic integration tests for
 * MqttClientControlPacketTest. It should not be considered complete,
 * but serve as a starting point for the student implementing
 * additional functionality within their Programming the IoT
 * environment.
 *
 */
public class MqttClientControlPacketTest
{
	// static
	
	private static final Logger _Logger =
		Logger.getLogger(MqttClientControlPacketTest.class.getName());
	
	
	// member var's
	
	private MqttClientConnector mqttClient = null;
	
	
	// test setup methods
	
	@Before
	public void setUp() throws Exception
	{
		this.mqttClient = new MqttClientConnector();
	}
	
	@After
	public void tearDown() throws Exception
	{
	}
	
	// test methods
	
	@Test
	public void testConnectAndDisconnect()
	{
        assertTrue(this.mqttClient.connectClient());

        try { Thread.sleep(2000); } 
		catch (Exception e) { }

        assertTrue(this.mqttClient.disconnectClient());
	}
	
	@Test
	public void testServerPing()
	{
	    int keepAlive = ConfigUtil.getInstance().getInteger(
        ConfigConst.MQTT_GATEWAY_SERVICE,
        ConfigConst.KEEP_ALIVE_KEY,
        ConfigConst.DEFAULT_KEEP_ALIVE);

        assertTrue(this.mqttClient.connectClient());

        _Logger.info("Waiting " + (keepAlive + 5) + " seconds for PINGREQ / PINGRESP...");

        try { Thread.sleep((keepAlive + 5) * 1000L); } 
		catch (Exception e) { }

        assertTrue(this.mqttClient.disconnectClient());
	}
	
	@Test
	public void testPubSub()
	{
		assertTrue(this.mqttClient.connectClient());

        assertTrue(this.mqttClient.subscribeToTopic(ResourceNameEnum.GDA_MGMT_STATUS_MSG_RESOURCE, 2));
        assertTrue(this.mqttClient.subscribeToTopic(ResourceNameEnum.CDA_ACTUATOR_RESPONSE_RESOURCE, 2));
        assertTrue(this.mqttClient.subscribeToTopic(ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE, 2));
        assertTrue(this.mqttClient.subscribeToTopic(ResourceNameEnum.CDA_SYSTEM_PERF_MSG_RESOURCE, 2));

        try { Thread.sleep(2000); } 
		catch (Exception e) { }

        // PUBLISH at QoS 1 -> PUBACK
        _Logger.info("Publishing messages at QoS 1 (expect PUBLISH + PUBACK)...");
        assertTrue(this.mqttClient.publishMessage(
            ResourceNameEnum.GDA_MGMT_STATUS_MSG_RESOURCE,
            "TEST QoS1: Control packet payload 1.", 1));
        assertTrue(this.mqttClient.publishMessage(
            ResourceNameEnum.GDA_MGMT_STATUS_MSG_RESOURCE,
            "TEST QoS1: Control packet payload 2.", 1));

        try { Thread.sleep(2000); } 
		catch (Exception e) { }

        // PUBLISH at QoS 2 -> PUBREC -> PUBREL -> PUBCOMP
        _Logger.info("Publishing messages at QoS 2 (expect PUBLISH + PUBREC + PUBREL + PUBCOMP)...");
        assertTrue(this.mqttClient.publishMessage(
            ResourceNameEnum.GDA_MGMT_STATUS_MSG_RESOURCE,
            "TEST QoS2: Control packet payload 1.", 2));
        assertTrue(this.mqttClient.publishMessage(
            ResourceNameEnum.GDA_MGMT_STATUS_MSG_RESOURCE,
            "TEST QoS2: Control packet payload 2.", 2));

        try { Thread.sleep(5000); } 
		catch (Exception e) { }

        // UNSUBSCRIBE -> UNSUBACK
        assertTrue(this.mqttClient.unsubscribeFromTopic(ResourceNameEnum.GDA_MGMT_STATUS_MSG_RESOURCE));
        assertTrue(this.mqttClient.unsubscribeFromTopic(ResourceNameEnum.CDA_ACTUATOR_RESPONSE_RESOURCE));
        assertTrue(this.mqttClient.unsubscribeFromTopic(ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE));
        assertTrue(this.mqttClient.unsubscribeFromTopic(ResourceNameEnum.CDA_SYSTEM_PERF_MSG_RESOURCE));

        try { Thread.sleep(2000); } 
		catch (Exception e) { }


        assertTrue(this.mqttClient.disconnectClient());
	}
	
}