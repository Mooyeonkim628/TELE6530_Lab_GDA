package programmingtheiot.gda.connection;

import java.util.logging.Logger;

import programmingtheiot.common.ConfigConst;
import programmingtheiot.common.ConfigUtil;

public class CloudClientFactory
{
	private static final Logger _Logger =
		Logger.getLogger(CloudClientFactory.class.getName());

	private static final CloudClientFactory _Instance =
		new CloudClientFactory();

	public static final CloudClientFactory getInstance()
	{
		return _Instance;
	}

	private CloudClientFactory()
	{
		super();
	}

	public ICloudClient createCloudClient()
	{
		ICloudClient cloudClient = null;

		ConfigUtil configUtil = ConfigUtil.getInstance();

		if (configUtil.hasProperty(
			ConfigConst.CLOUD_GATEWAY_SERVICE,
			ConfigConst.CLOUD_SERVICE_NAME_KEY)) {

			String cloudSvcName =
				configUtil.getProperty(
					ConfigConst.CLOUD_GATEWAY_SERVICE,
					ConfigConst.CLOUD_SERVICE_NAME_KEY);

			if (cloudSvcName != null && cloudSvcName.trim().length() > 0) {
				_Logger.info(
					"Attempting to instantiate cloud client using cloud service name: " +
					cloudSvcName);

				if (cloudSvcName.equalsIgnoreCase(ConfigConst.UBIDOTS_CLOUD_SVC_NAME)) {
					cloudClient = new UbidotsCloudClientConnector();
				} else {
					_Logger.warning(
						"Cloud service name not recognized. Failed to create client for: " +
						cloudSvcName);
				}
			}
		}

		if (cloudClient == null) {
			_Logger.warning(
				"No valid cloud service provider specified in configuration. " +
				"Failed to create ICloudClient instance.");
		}

		return cloudClient;
	}
}