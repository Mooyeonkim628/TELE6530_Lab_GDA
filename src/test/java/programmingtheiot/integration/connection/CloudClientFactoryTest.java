package programmingtheiot.integration.connection;

import java.util.logging.Logger;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

import programmingtheiot.gda.connection.CloudClientFactory;
import programmingtheiot.gda.connection.ICloudClient;

public class CloudClientFactoryTest
{
	private static final Logger _Logger =
		Logger.getLogger(CloudClientFactoryTest.class.getName());

	@Test
	public void testCreateCloudClient()
	{
		CloudClientFactory factory = CloudClientFactory.getInstance();

		assertNotNull("CloudClientFactory should not be null.", factory);

		ICloudClient cloudClient = factory.createCloudClient();

		assertNotNull(
			"CloudClientFactory should create a non-null ICloudClient instance.",
			cloudClient
		);
	}

	@Test
	public void testCreateAndTestCloudClient()
	{
		CloudClientFactory factory = CloudClientFactory.getInstance();

		assertNotNull("CloudClientFactory should not be null.", factory);

		ICloudClient cloudClient = factory.createCloudClient();

		assertNotNull(
			"CloudClientFactory should create a non-null ICloudClient instance.",
			cloudClient
		);

		boolean connected = cloudClient.connectClient();

		assertTrue(
			"ICloudClient should connect successfully using PiotConfig.props.",
			connected
		);

		_Logger.info("Cloud client connected successfully.");

		boolean disconnected = cloudClient.disconnectClient();

		assertTrue(
			"ICloudClient should disconnect successfully.",
			disconnected
		);

		_Logger.info("Cloud client disconnected successfully.");
	}
}