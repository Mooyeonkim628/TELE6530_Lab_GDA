package programmingtheiot.gda.connection;

import programmingtheiot.common.ConfigConst;

public class UbidotsCloudClientConnector extends CloudClientConnector
{
	public UbidotsCloudClientConnector()
	{
		super(ConfigConst.UBIDOTS_CLOUD_GATEWAY_SERVICE);
	}

	public UbidotsCloudClientConnector(String cloudGatewaySectionName)
	{
		super(cloudGatewaySectionName);
	}
}