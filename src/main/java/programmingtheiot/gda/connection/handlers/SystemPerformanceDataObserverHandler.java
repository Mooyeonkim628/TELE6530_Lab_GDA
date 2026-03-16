package programmingtheiot.gda.connection.handlers;
import java.util.logging.Logger;

import org.eclipse.californium.core.CoapHandler;
import org.eclipse.californium.core.CoapResponse;

import programmingtheiot.common.IDataMessageListener;

public class SystemPerformanceDataObserverHandler implements CoapHandler
{
	private static final Logger _Logger =
		Logger.getLogger(SystemPerformanceDataObserverHandler.class.getName());

	private IDataMessageListener dataMsgListener = null;

	public SystemPerformanceDataObserverHandler()
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
			_Logger.info("Received CoAP response (payload should be PerformanceData in JSON): " + response.getResponseText());
		}
	}
}