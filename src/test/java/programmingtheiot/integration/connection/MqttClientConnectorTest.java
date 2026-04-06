package programmingtheiot.integration.connection;

import org.junit.After;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

import programmingtheiot.common.DefaultDataMessageListener;
import programmingtheiot.common.ResourceNameEnum;
import programmingtheiot.gda.connection.MqttClientConnector;

public class MqttClientConnectorTest
{
    private MqttClientConnector mqttClient = null;

    @Before
    public void setUp() throws Exception
    {
        this.mqttClient = new MqttClientConnector();
        this.mqttClient.setDataMessageListener(new DefaultDataMessageListener());
    }

    @After
    public void tearDown() throws Exception
    {
        if (this.mqttClient != null) {
            try {
                if (this.mqttClient.isConnected()) {
                    this.mqttClient.disconnectClient();
                    Thread.sleep(1000L);
                }
            } catch (Exception e) {
                // ignore
            }
        }
    }

    @Test
    public void testConnectAndDisconnect()
    {
        assertTrue(this.mqttClient.connectClient());
        sleep(1000L);

        assertTrue(this.mqttClient.isConnected());

        assertTrue(this.mqttClient.disconnectClient());
        sleep(1000L);
    }

    @Test
    public void testSubscribeAndUnsubscribe()
    {
        assertTrue(this.mqttClient.connectClient());
        sleep(1000L);

        assertTrue(
            this.mqttClient.subscribeToTopic(
                ResourceNameEnum.CDA_ACTUATOR_RESPONSE_RESOURCE,
                0
            )
        );

        sleep(1000L);

        assertTrue(
            this.mqttClient.unsubscribeFromTopic(
                ResourceNameEnum.CDA_ACTUATOR_RESPONSE_RESOURCE
            )
        );

        sleep(1000L);

        assertTrue(this.mqttClient.disconnectClient());
        sleep(1000L);
    }

    @Test
    public void testPublishActuatorCmdTopic()
    {
        assertTrue(this.mqttClient.connectClient());
        sleep(1000L);

        String payload =
            "{\"command\":1,\"value\":1.0,\"isResponse\":false,\"stateData\":\"LED switching ON\",\"name\":\"LedActuator\"}";

        assertTrue(
            this.mqttClient.publishMessage(
                ResourceNameEnum.CDA_ACTUATOR_CMD_RESOURCE,
                payload,
                0
            )
        );

        sleep(1000L);

        assertTrue(this.mqttClient.disconnectClient());
        sleep(1000L);
    }

    private void sleep(long millis)
    {
        try {
            Thread.sleep(millis);
        } catch (Exception e) {
            // ignore
        }
    }
}
