/**
 * 
 * This class is part of the Programming the Internet of Things
 * project, and is available via the MIT License, which can be
 * found in the LICENSE file at the top level of this repository.
 * 
 * Copyright (c) 2020 - 2025 by Andrew D. King
 */ 

package programmingtheiot.integration.connection;

import java.util.Date;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import programmingtheiot.common.ConfigConst;
import programmingtheiot.common.ResourceNameEnum;
import programmingtheiot.data.ActuatorData;
import programmingtheiot.data.SensorData;
import programmingtheiot.data.SystemPerformanceData;
import programmingtheiot.gda.connection.RedisPersistenceAdapter;

/**
 * This test case class contains very basic integration tests for
 * RedisPersistenceAdapter. It should not be considered complete,
 * but serve as a starting point for the student implementing
 * additional functionality within their Programming the IoT
 * environment.
 *
 */
public class PersistenceClientAdapterTest
{
	// static
	private static RedisPersistenceAdapter client = null;
	private static final Logger _Logger =
		Logger.getLogger(PersistenceClientAdapterTest.class.getName());
	
	
	// member var's
	
	private RedisPersistenceAdapter rpa = null;
	
	
	// test setup methods
	
	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception
	{
		client = new RedisPersistenceAdapter();
	}
	
	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception
	{
		if (client != null) {
			client.disconnectClient();
		}
	}
	
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception
	{
	}
	
	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception
	{
	}
	
	// test methods
	
	/**
	 * Test method for {@link programmingtheiot.gda.connection.RedisPersistenceAdapter#connectClient()}.
	 */
	@Test
	public void testConnectClient()
	{
		assertNotNull(client);
		assertTrue(client.connectClient());
		assertTrue(client.connectClient());
	}
	
	/**
	 * Test method for {@link programmingtheiot.gda.connection.RedisPersistenceAdapter#disconnectClient()}.
	 */
	@Test
	public void testDisconnectClient()
	{
		assertNotNull(client);
		assertTrue(client.connectClient());
		assertTrue(client.disconnectClient());
		assertTrue(client.disconnectClient());
	}
	
	/**
	 * Test method for {@link programmingtheiot.gda.connection.RedisPersistenceAdapter#getActuatorData(java.lang.String, java.util.Date, java.util.Date)}.
	 */
	@Test
	public void testGetActuatorData()
	{
		assertTrue(client.connectClient());

		String topic = ResourceNameEnum.CDA_ACTUATOR_RESPONSE_RESOURCE.getResourceName();


		ActuatorData ad = new ActuatorData();
		ad.setName("testActuator");
		ad.setCommand(1);
		ad.setValue(12.34f);

		assertTrue(client.storeData(topic, ConfigConst.DEFAULT_QOS, ad));


		Date start = new Date(System.currentTimeMillis() - 60_000L);
		Date end   = new Date(System.currentTimeMillis() + 60_000L);

		ActuatorData[] arr = client.getActuatorData(topic, start, end);

		assertNotNull(arr);
		assertTrue(arr.length > 0);

		assertTrue(client.disconnectClient());
	}
	
	/**
	 * Test method for {@link programmingtheiot.gda.connection.RedisPersistenceAdapter#getSensorData(java.lang.String, java.util.Date, java.util.Date)}.
	 */
	@Test
	public void testGetSensorData()
	{
		assertTrue(client.connectClient());

		String topic = ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE.getResourceName();

		SensorData sd = new SensorData();
		sd.setName("testSensor");
		sd.setValue(77.7f);

		assertTrue(client.storeData(topic, ConfigConst.DEFAULT_QOS, sd));


		Date start = new Date(System.currentTimeMillis() - 60_000L);
		Date end   = new Date(System.currentTimeMillis() + 60_000L);

		SensorData[] arr = client.getSensorData(topic, start, end);

		assertNotNull(arr);
		assertTrue(arr.length > 0);

		assertTrue(client.disconnectClient());
	}
	
	/**
	 * Test method for {@link programmingtheiot.gda.connection.RedisPersistenceAdapter#storeData(java.lang.String, int, programmingtheiot.data.ActuatorData[])}.
	 */
	@Test
	public void testStoreDataStringIntActuatorDataArray()
	{
				assertTrue(client.connectClient());

		String topic = ResourceNameEnum.CDA_ACTUATOR_RESPONSE_RESOURCE.getResourceName();

		ActuatorData a1 = new ActuatorData();
		a1.setName("a1");
		a1.setCommand(1);
		a1.setValue(1.0f);

		ActuatorData a2 = new ActuatorData();
		a2.setName("a2");
		a2.setCommand(0);
		a2.setValue(0.0f);

		assertTrue(client.storeData(topic, ConfigConst.DEFAULT_QOS, a1,a2));
		assertTrue(client.disconnectClient());
	}
	
	/**
	 * Test method for {@link programmingtheiot.gda.connection.RedisPersistenceAdapter#storeData(java.lang.String, int, programmingtheiot.data.SensorData[])}.
	 */
	@Test
	public void testStoreDataStringIntSensorDataArray()
	{
		assertTrue(client.connectClient());

		String topic = ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE.getResourceName();

		SensorData s1 = new SensorData();
		s1.setName("s1");
		s1.setValue(10.1f);

		SensorData s2 = new SensorData();
		s2.setName("s2");
		s2.setValue(20.2f);

		assertTrue(client.storeData(topic, ConfigConst.DEFAULT_QOS, s1,s2));
		assertTrue(client.disconnectClient());
	}
	
	/**
	 * Test method for {@link programmingtheiot.gda.connection.RedisPersistenceAdapter#storeData(java.lang.String, int, programmingtheiot.data.SystemPerformanceData[])}.
	 */
	@Test
	public void testStoreDataStringIntSystemPerformanceDataArray()
	{
		assertTrue(client.connectClient());

		String topic = ResourceNameEnum.GDA_SYSTEM_PERF_MSG_RESOURCE.getResourceName();

		SystemPerformanceData sp1 = new SystemPerformanceData();
		sp1.setName("sp1");
		sp1.setCpuUtilization(11.1f);
		sp1.setDiskUtilization(22.2f);
		sp1.setMemoryUtilization(33.3f);

		SystemPerformanceData sp2 = new SystemPerformanceData();
		sp2.setName("sp2");
		sp2.setCpuUtilization(44.4f);
		sp2.setDiskUtilization(55.5f);
		sp2.setMemoryUtilization(66.6f);

		assertTrue(client.storeData(topic, ConfigConst.DEFAULT_QOS, sp1,sp2));
		assertTrue(client.disconnectClient());
	}
	
}
