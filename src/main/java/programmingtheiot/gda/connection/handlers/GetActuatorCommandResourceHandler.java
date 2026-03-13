package programmingtheiot.gda.connection.handlers;

import java.util.logging.Logger;

import org.eclipse.californium.core.CoapResource;
import org.eclipse.californium.core.coap.CoAP.ResponseCode;
import org.eclipse.californium.core.server.resources.CoapExchange;

import programmingtheiot.common.IActuatorDataListener;
import programmingtheiot.data.ActuatorData;
import programmingtheiot.data.DataUtil;

public class GetActuatorCommandResourceHandler extends CoapResource
	implements IActuatorDataListener
{
	private static final Logger _Logger =
		Logger.getLogger(GetActuatorCommandResourceHandler.class.getName());

	private ActuatorData actuatorData = null;

	public GetActuatorCommandResourceHandler(String resourceName)
	{
		super(resourceName);

		super.setObservable(true);

		this.actuatorData = new ActuatorData();

		_Logger.info("Resource handler created with name: " + resourceName);
	}

	@Override
	public boolean onActuatorDataUpdate(ActuatorData data)
	{
		if (data != null && this.actuatorData != null) {
			this.actuatorData.updateData(data);

			super.changed();

			_Logger.fine(
				"Actuator data updated for URI: " +
				super.getURI() +
				": Data value = " +
				this.actuatorData.getValue()
			);

			return true;
		}

		return false;
	}

	@Override
	public void handleGET(CoapExchange context)
	{
		context.accept();

		try {
			String jsonData =
				DataUtil.getInstance().actuatorDataToJson(this.actuatorData);

			_Logger.info(
				"GET called on actuator command resource: " +
				super.getName() +
				", payload: " +
				jsonData
			);

			context.respond(ResponseCode.CONTENT, jsonData);
		} catch (Exception e) {
			_Logger.warning(
				"Failed to handle GET request for actuator command resource. Message: " +
				e.getMessage()
			);

			context.respond(
				ResponseCode.INTERNAL_SERVER_ERROR,
				"Failed to process actuator command GET request."
			);
		}
	}
}