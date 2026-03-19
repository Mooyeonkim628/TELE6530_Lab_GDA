package programmingtheiot.gda.connection.handlers;
import java.util.logging.Logger;

import org.eclipse.californium.core.CoapHandler;
import org.eclipse.californium.core.CoapResponse;

import programmingtheiot.common.IDataMessageListener;

public class SensorDataObserverHandler implements CoapHandler
{
	private static final Logger _Logger =
		Logger.getLogger(SensorDataObserverHandler.class.getName());

	private IDataMessageListener dataMsgListener = null;

	public SensorDataObserverHandler()
	{
		super();
	}

	public void setDataMessageListener(IDataMessageListener listener)
	{
		this.dataMsgListener = listener;
	}

	@Override
	public void onError()
	{
		_Logger.warning("Handling CoAP error...");
	}

	@Override
	public void onLoad(CoapResponse response)
	{
		if (response != null) {
			_Logger.info("Received CoAP response (payload should be SensorData in JSON): " + response.getResponseText());
		}
	}
}