package programmingtheiot.gda.connection.handlers;

import java.util.logging.Logger;

import org.eclipse.californium.core.CoapResource;
import org.eclipse.californium.core.coap.CoAP.ResponseCode;
import org.eclipse.californium.core.server.resources.CoapExchange;

import programmingtheiot.common.IDataMessageListener;
import programmingtheiot.common.ResourceNameEnum;
import programmingtheiot.data.DataUtil;
import programmingtheiot.data.SystemPerformanceData;

public class UpdateSystemPerformanceResourceHandler extends CoapResource
{
	private static final Logger _Logger =
		Logger.getLogger(UpdateSystemPerformanceResourceHandler.class.getName());

	private IDataMessageListener dataMsgListener = null;

	public UpdateSystemPerformanceResourceHandler(String resourceName)
	{
		super(resourceName);
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

				SystemPerformanceData sysPerfData =
					DataUtil.getInstance().jsonToSystemPerformanceData(jsonData);

				this.dataMsgListener.handleSystemPerformanceMessage(
					ResourceNameEnum.CDA_SYSTEM_PERF_MSG_RESOURCE,
					sysPerfData
				);

				code = ResponseCode.CHANGED;
			} catch (Exception e) {
				_Logger.warning(
					"Failed to handle PUT request for system performance data. Message: " +
					e.getMessage()
				);

				code = ResponseCode.BAD_REQUEST;
			}
		} else {
			_Logger.info("No callback listener for request. Ignoring PUT.");
			code = ResponseCode.CONTINUE;
		}

		String msg =
			"Update system perf data request handled: " + super.getName();

		context.respond(code, msg);
	}

	@Override
	public void handleGET(CoapExchange context)
	{
		_Logger.info("GET called on resource: " + super.getName());
		context.respond(ResponseCode.CONTENT, "Generic handler. No GET action taken: " + super.getName());
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