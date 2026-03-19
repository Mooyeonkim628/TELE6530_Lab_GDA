package programmingtheiot.gda.connection.handlers;

import java.util.logging.Logger;

import org.eclipse.californium.core.CoapResource;
import org.eclipse.californium.core.coap.CoAP.ResponseCode;
import org.eclipse.californium.core.server.resources.CoapExchange;

import programmingtheiot.common.IDataMessageListener;
import programmingtheiot.common.ResourceNameEnum;
import programmingtheiot.data.DataUtil;
import programmingtheiot.data.SensorData;

public class UpdateTelemetryResourceHandler extends CoapResource
{
	private static final Logger _Logger =
		Logger.getLogger(UpdateTelemetryResourceHandler.class.getName());

	private IDataMessageListener dataMsgListener = null;
	private SensorData sensorData = new SensorData();

	public UpdateTelemetryResourceHandler(String resourceName)
	{
		super(resourceName);
		this.sensorData.setName("UNINITIALIZED_GDA_SENSOR"); //test
	}

	public void setDataMessageListener(IDataMessageListener listener)
	{
		if (listener != null) {
			this.dataMsgListener = listener;
		}
	}

	@Override
	public void handlePUT(CoapExchange context)
	{
		ResponseCode code = ResponseCode.NOT_ACCEPTABLE;

		context.accept();

		if (this.dataMsgListener != null) {
			try {
				String jsonData = new String(context.getRequestPayload());

				SensorData sensorData =
					DataUtil.getInstance().jsonToSensorData(jsonData);

				this.sensorData = sensorData;

				_Logger.info("Received SensorData via PUT: " + jsonData);
				_Logger.info("Stored SensorData name: " + this.sensorData.getName());

				this.dataMsgListener.handleSensorMessage(
					ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE,
					sensorData
				);

				code = ResponseCode.CHANGED;
			} catch (Exception e) {
				_Logger.warning(
					"Failed to handle PUT request for telemetry data. Message: " +
					e.getMessage()
				);

				code = ResponseCode.BAD_REQUEST;
			}
		} else {
			_Logger.info("No callback listener for request. Ignoring PUT.");
			code = ResponseCode.CONTINUE;
		}

		String msg =
			"Update telemetry data request handled: " + super.getName();

		context.respond(code, msg);
	}

	@Override
	public void handleGET(CoapExchange context)
	{
		try {
			String jsonData =
				DataUtil.getInstance().sensorDataToJson(this.sensorData);

			_Logger.info(
				"GET called on resource: " + super.getName() +
				", payload: " + jsonData
			);

			context.respond(ResponseCode.CONTENT, jsonData);
		} catch (Exception e) {
			_Logger.warning(
				"Failed to handle GET request for telemetry data. Message: " +
				e.getMessage()
			);

			context.respond(
				ResponseCode.INTERNAL_SERVER_ERROR,
				"Failed to process telemetry GET request."
			);
		}
	}

	@Override
	public void handlePOST(CoapExchange context)
	{
		_Logger.info("POST called on resource: " + super.getName());
		context.accept();
		context.respond(ResponseCode.CREATED, "POST handled for " + super.getName());
	}

	@Override
	public void handleDELETE(CoapExchange context)
	{
		_Logger.info("DELETE called on resource: " + super.getName());
		context.respond(ResponseCode.DELETED, "DELETE handled for " + super.getName());
	}
}