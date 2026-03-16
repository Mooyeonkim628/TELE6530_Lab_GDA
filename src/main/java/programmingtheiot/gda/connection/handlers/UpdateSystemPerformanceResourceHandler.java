package programmingtheiot.gda.connection.handlers;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
	private final ScheduledExecutorService scheduler =
		Executors.newSingleThreadScheduledExecutor();

	private IDataMessageListener dataMsgListener = null;
	private SystemPerformanceData sysPerfData = new SystemPerformanceData();
	private float cpuVal = 0.0f;
	private float memVal = 0.0f;
	private float diskVal = 0.0f;

	public UpdateSystemPerformanceResourceHandler(String resourceName)
	{
		super(resourceName);

		super.setObservable(true);

		startTestUpdates();
	}

	private void startTestUpdates()
	{
		this.scheduler.scheduleAtFixedRate(() -> {
			try {
				cpuVal += 1.0f;
				memVal += 1.0f;
				diskVal += 1.0f;

				SystemPerformanceData data = new SystemPerformanceData();
				data.setCpuUtilization(cpuVal);
				data.setMemoryUtilization(memVal);
				data.setDiskUtilization(diskVal);

				this.sysPerfData = data;

				super.changed();

				_Logger.info(
					"System performance data updated for observe: " +
					DataUtil.getInstance().systemPerformanceDataToJson(this.sysPerfData));
			} catch (Exception e) {
				_Logger.warning("Failed to update system performance test data: " + e.getMessage());
			}
		}, 5, 5, TimeUnit.SECONDS);
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

				SystemPerformanceData data =
					DataUtil.getInstance().jsonToSystemPerformanceData(jsonData);

				this.sysPerfData = data;

				this.dataMsgListener.handleSystemPerformanceMessage(
					ResourceNameEnum.CDA_SYSTEM_PERF_MSG_RESOURCE,
					data
				);

				super.changed();

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
		try {
			String jsonData =
				DataUtil.getInstance().systemPerformanceDataToJson(this.sysPerfData);

			_Logger.info("GET called on resource: " + super.getName() + ", payload: " + jsonData);

			context.respond(ResponseCode.CONTENT, jsonData);
		} catch (Exception e) {
			_Logger.warning(
				"Failed to handle GET request for system performance data. Message: " +
				e.getMessage()
			);

			context.respond(
				ResponseCode.INTERNAL_SERVER_ERROR,
				"Failed to process system performance GET request."
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