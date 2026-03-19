package programmingtheiot.gda.connection.handlers;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
	private final ScheduledExecutorService scheduler =
		Executors.newSingleThreadScheduledExecutor();

	private float[] testValues = new float[] {68.0f, 5.0f, 0.0f};
	private int idx = 0;
	private boolean startedTestUpdates = false;

	public GetActuatorCommandResourceHandler(String resourceName)
	{
		super(resourceName);

		super.setObservable(true);

		this.actuatorData = new ActuatorData();
		this.actuatorData.setCommand(0);
		this.actuatorData.setValue(0.0f);
		this.actuatorData.setStateData("This is a test.");

		startTestUpdates();

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
			if (!this.startedTestUpdates) {
				this.startedTestUpdates = true;
				startTestUpdates();
			}

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
	private void startTestUpdates(){
		this.scheduler.scheduleAtFixedRate(() -> {
			try {
				ActuatorData ad = new ActuatorData();
				ad.setCommand(0);
				ad.setValue(this.testValues[this.idx]);
				ad.setStateData("TESTING PURPOSES!!");

				this.idx = (this.idx + 1) % this.testValues.length;

				onActuatorDataUpdate(ad);
			} catch (Exception e) {
				_Logger.warning("Failed to update test actuator data: " + e.getMessage());
			}
		}, 5, 5, TimeUnit.SECONDS);
	}
}