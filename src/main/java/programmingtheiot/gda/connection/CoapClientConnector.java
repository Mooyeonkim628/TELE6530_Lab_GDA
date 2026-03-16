/**
 * This class is part of the Programming the Internet of Things
 * project, and is available via the MIT License, which can be
 * found in the LICENSE file at the top level of this repository.
 * 
 * You may find it more helpful to your design to adjust the
 * functionality, constants and interfaces (if there are any)
 * provided within in order to meet the needs of your specific
 * Programming the Internet of Things project.
 */

package programmingtheiot.gda.connection;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eclipse.californium.core.CoapClient;
import org.eclipse.californium.core.CoapHandler;
import org.eclipse.californium.core.CoapObserveRelation;
import org.eclipse.californium.core.CoapResponse;
import org.eclipse.californium.core.WebLink;
import org.eclipse.californium.core.coap.MediaTypeRegistry;
import org.eclipse.californium.core.config.CoapConfig;
import org.eclipse.californium.elements.config.Configuration;
import org.eclipse.californium.elements.config.UdpConfig;

import programmingtheiot.common.ConfigConst;
import programmingtheiot.common.ConfigUtil;
import programmingtheiot.common.IDataMessageListener;
import programmingtheiot.common.ResourceNameEnum;
import programmingtheiot.gda.connection.handlers.SensorDataObserverHandler;
import programmingtheiot.gda.connection.handlers.SystemPerformanceDataObserverHandler;
/**
 * Shell representation of class for student implementation.
 *
 */
public class CoapClientConnector implements IRequestResponseClient
{
	// static
	static {
		CoapConfig.register();
		UdpConfig.register();
	}
	private static final Logger _Logger =
		Logger.getLogger(CoapClientConnector.class.getName());
	
	// params
	private String     protocol;
	private String     host;
	private int        port;
	private String     serverAddr;
	private CoapClient clientConn;
	private IDataMessageListener dataMsgListener;
	private Map<String, CoapObserveRelation> observeRelationMap = new HashMap<>();
	// constructors
	
	/**
	 * Default.
	 * 
	 * All config data will be loaded from the config file.
	 */
	public CoapClientConnector()
	{
		super();

		ConfigUtil config = ConfigUtil.getInstance();

		this.host =
			config.getProperty(
				ConfigConst.COAP_GATEWAY_SERVICE,
				ConfigConst.HOST_KEY,
				ConfigConst.DEFAULT_HOST);

		if (config.getBoolean(
				ConfigConst.COAP_GATEWAY_SERVICE,
				ConfigConst.ENABLE_CRYPT_KEY))
		{
			this.protocol = ConfigConst.DEFAULT_COAP_SECURE_PROTOCOL;
			this.port =
				config.getInteger(
					ConfigConst.COAP_GATEWAY_SERVICE,
					ConfigConst.SECURE_PORT_KEY,
					ConfigConst.DEFAULT_COAP_SECURE_PORT);
		}
		else
		{
			this.protocol = ConfigConst.DEFAULT_COAP_PROTOCOL;
			this.port =
				config.getInteger(
					ConfigConst.COAP_GATEWAY_SERVICE,
					ConfigConst.PORT_KEY,
					ConfigConst.DEFAULT_COAP_PORT);
		}

		this.serverAddr = this.protocol + "://" + this.host + ":" + this.port;

		initClient();

		_Logger.info("Using URL for server conn: " + this.serverAddr);
	}
		
	/**
	 * Constructor.
	 * 
	 * @param host
	 * @param isSecure
	 * @param enableConfirmedMsgs
	 */
	public CoapClientConnector(String host, boolean isSecure, boolean enableConfirmedMsgs)
	{
		super();

		this.host = host;

		if (isSecure)
		{
			this.protocol = ConfigConst.DEFAULT_COAP_SECURE_PROTOCOL;
			this.port = ConfigConst.DEFAULT_COAP_SECURE_PORT;
		}
		else
		{
			this.protocol = ConfigConst.DEFAULT_COAP_PROTOCOL;
			this.port = ConfigConst.DEFAULT_COAP_PORT;
		}

		this.serverAddr = this.protocol + "://" + this.host + ":" + this.port;

		initClient();

		_Logger.info("Using URL for server conn: " + this.serverAddr);
	}
	
	
	// public methods
	

	@Override
	public boolean sendDiscoveryRequest(int timeout)
	{
		try {
			_Logger.info("Issuing discover...");

			Set<WebLink> wlSet = this.clientConn.discover();

			_Logger.info(" --> URI: /.well-known/core. Attributes: " +
				new org.eclipse.californium.core.server.resources.ResourceAttributes());

			if (wlSet != null) {
				for (WebLink wl : wlSet) {
					_Logger.info(" --> URI: " + wl.getURI() + ". Attributes: " + wl.getAttributes());
				}

				return true;
			} else {
				_Logger.warning("Discovery returned no resources.");
			}
		} catch (Exception e) {
			_Logger.log(Level.SEVERE, "Failed to issue discovery request.", e);
		}

		return false;
	}

	@Override
	public boolean sendDeleteRequest(ResourceNameEnum resource, String name, boolean enableCON, int timeout)
	{
		CoapResponse response = null;

		try {
			if (resource == null) {
				_Logger.warning("Handling DELETE. No resource provided.");
				return false;
			}

			if (enableCON) {
				this.clientConn.useCONs();
			} else {
				this.clientConn.useNONs();
			}

			String uri = this.serverAddr + "/" + resource.getResourceName();

			if (name != null && name.length() > 0) {
				uri += "/" + name;
			}

			this.clientConn.setURI(uri);
			response = this.clientConn.delete();

			if (response != null) {
				_Logger.info(
					"Handling DELETE. Response: " +
					response.isSuccess() + " - " +
					response.getOptions() + " - " +
					response.getCode() + " - " +
					response.getResponseText());

				if (this.dataMsgListener != null) {
				}

				return true;
			} else {
				_Logger.warning("Handling DELETE. No response received.");
			}
		} catch (Exception e) {
			_Logger.log(Level.SEVERE, "Failed to issue DELETE request.", e);
		}

		return false;
	}

	@Override
	public boolean sendGetRequest(ResourceNameEnum resource, String name, boolean enableCON, int timeout)
	{
		CoapResponse response = null;

		try {
			if (resource == null) {
				_Logger.warning("Handling GET. No resource provided.");
				return false;
			}

			if (enableCON) {
				this.clientConn.useCONs();
			} else {
				this.clientConn.useNONs();
			}

			String uri = this.serverAddr + "/" + resource.getResourceName();

			if (name != null && name.length() > 0) {
				uri += "/" + name;
			}

			this.clientConn.setURI(uri);

			response = this.clientConn.get();

			if (response != null) {
				_Logger.info(
					"Handling GET. Response: " +
					response.isSuccess() + " - " +
					response.getOptions() + " - " +
					response.getCode() + " - " +
					response.getResponseText());

	
				if (this.dataMsgListener != null) {

				}

				return true;
			} else {
				_Logger.warning("Handling GET. No response received.");
			}
		} catch (Exception e) {
			_Logger.log(Level.SEVERE, "Failed to issue GET request.", e);
		}

		return false;
	}

	@Override
	public boolean sendPostRequest(ResourceNameEnum resource, String name, boolean enableCON, String payload, int timeout)
	{
		CoapResponse response = null;

		try {
			if (resource == null) {
				_Logger.warning("Handling POST. No resource provided.");
				return false;
			}

			if (enableCON) {
				this.clientConn.useCONs();
			} else {
				this.clientConn.useNONs();
			}

			String uri = this.serverAddr + "/" + resource.getResourceName();

			if (name != null && name.length() > 0) {
				uri += "/" + name;
			}

			this.clientConn.setURI(uri);

			response = this.clientConn.post(payload, MediaTypeRegistry.APPLICATION_JSON);

			if (response != null) {
				_Logger.info(
					"Handling POST. Response: " +
					response.isSuccess() + " - " +
					response.getOptions() + " - " +
					response.getCode() + " - " +
					response.getResponseText());

				if (this.dataMsgListener != null) {
				}

				return true;
			} else {
				_Logger.warning("Handling POST. No response received.");
			}
		} catch (Exception e) {
			_Logger.log(Level.SEVERE, "Failed to issue POST request.", e);
		}

		return false;
	}

	@Override
	public boolean sendPutRequest(ResourceNameEnum resource, String name, boolean enableCON, String payload, int timeout)
	{
		CoapResponse response = null;

		try {
			if (resource == null) {
				_Logger.warning("Handling PUT. No resource provided.");
				return false;
			}

			if (enableCON) {
				this.clientConn.useCONs();
			} else {
				this.clientConn.useNONs();
			}

			String uri = this.serverAddr + "/" + resource.getResourceName();

			if (name != null && name.length() > 0) {
				uri += "/" + name;
			}

			this.clientConn.setURI(uri);
			response = this.clientConn.put(payload, MediaTypeRegistry.APPLICATION_JSON);

			if (response != null) {
				_Logger.info(
					"Handling PUT. Response: " +
					response.isSuccess() + " - " +
					response.getOptions() + " - " +
					response.getCode() + " - " +
					response.getResponseText());

				if (this.dataMsgListener != null) {
				}

				return true;
			} else {
				_Logger.warning("Handling PUT. No response received.");
			}
		} catch (Exception e) {
			_Logger.log(Level.SEVERE, "Failed to issue PUT request.", e);
		}

		return false;
	}

	@Override
	public boolean setDataMessageListener(IDataMessageListener listener)
	{
		if (listener != null)
		{
			this.dataMsgListener = listener;
			return true;
		}

		return false;
	}

	public void clearEndpointPath()
	{
		_Logger.info("clearEndpointPath() called.");
	}
	
	public void setEndpointPath(ResourceNameEnum resource)
	{
		_Logger.info("setEndpointPath() called for resource: " + resource);
	}
	
	@Override
	public boolean startObserver(ResourceNameEnum resource, String name, int ttl)
	{
		try {
			if (resource == null) {
				_Logger.warning("No resource provided for observation.");
				return false;
			}

			String uriPath = this.serverAddr + "/" + resource.getResourceName();

			if (name != null && name.length() > 0) {
				uriPath += "/" + name;
			}

			_Logger.info("Observing resource [START]: " + uriPath);

			this.clientConn.setURI(uriPath);

			CoapHandler handler = null;

			if (resource == ResourceNameEnum.CDA_SENSOR_MSG_RESOURCE) {
				SensorDataObserverHandler sensorHandler = new SensorDataObserverHandler();
				sensorHandler.setDataMessageListener(this.dataMsgListener);
				handler = sensorHandler;
			} else if (
				resource == ResourceNameEnum.CDA_SYSTEM_PERF_MSG_RESOURCE ||
				resource == ResourceNameEnum.GDA_MGMT_STATUS_MSG_RESOURCE
			) {
				SystemPerformanceDataObserverHandler sysPerfHandler = new SystemPerformanceDataObserverHandler();
				sysPerfHandler.setDataMessageListener(this.dataMsgListener);
				handler = sysPerfHandler;
			} else {
				_Logger.warning("No observer handler defined for resource: " + resource);
				return false;
			}

			CoapObserveRelation cor = this.clientConn.observe(handler);

			if (cor != null) {
				this.observeRelationMap.put(resource.name(), cor);
				return !cor.isCanceled();
			}

		} catch (Exception e) {
			_Logger.log(Level.SEVERE, "Failed to start observation.", e);
		}

		return false;
	}

	@Override
	public boolean stopObserver(ResourceNameEnum resource, String name, int timeout)
	{
		if (resource == null) {
			_Logger.warning("No resource provided to stop observation.");
			return false;
		}

		CoapObserveRelation cor = this.observeRelationMap.get(resource.name());

		if (cor != null) {
			_Logger.info("Observing resource [STOP]: " + resource);
			cor.proactiveCancel();
			this.observeRelationMap.remove(resource.name());
			return true;
		}

		_Logger.warning("No active observer relation found for resource: " + resource);
		return false;
	}

	
	// private methods
	
	private void initClient()
	{
		try
		{
			if (Configuration.getStandard() == null)
			{
				Configuration config = new Configuration(
					CoapConfig.DEFINITIONS,
					UdpConfig.DEFINITIONS
				);

				Configuration.setStandard(config);

				_Logger.info("Initialized Californium configuration with standard definitions");
			}

			this.clientConn = new CoapClient(this.serverAddr);

			_Logger.info("Created client connection to server / resource: " + this.serverAddr);
		}
		catch (Exception e)
		{
			_Logger.log(Level.SEVERE, "Failed to connect to server: " + this.serverAddr, e);
		}
	}
	


}


