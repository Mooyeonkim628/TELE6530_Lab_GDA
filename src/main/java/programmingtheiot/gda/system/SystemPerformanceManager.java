package programmingtheiot.gda.system;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import programmingtheiot.common.ConfigConst;
import programmingtheiot.common.ConfigUtil;
import programmingtheiot.common.IDataMessageListener;
import programmingtheiot.common.ResourceNameEnum;
import programmingtheiot.data.SystemPerformanceData;

public class SystemPerformanceManager
{
	private static final Logger _Logger =
		Logger.getLogger(SystemPerformanceManager.class.getName());

	private int pollRate = ConfigConst.DEFAULT_POLL_CYCLES;
	private ScheduledExecutorService schedExecSvc = null;
	private SystemCpuUtilTask sysCpuUtilTask = null;
	private SystemMemUtilTask sysMemUtilTask = null;
	private SystemDiscUtilTask sysDiskUtilTask = null;

	private Runnable taskRunner = null;
	private boolean isStarted = false;
	private String locationID = ConfigConst.NOT_SET;
	private IDataMessageListener dataMsgListener = null;

	public SystemPerformanceManager()
	{
		this.pollRate =
			ConfigUtil.getInstance().getInteger(
				ConfigConst.GATEWAY_DEVICE,
				ConfigConst.POLL_CYCLES_KEY,
				ConfigConst.DEFAULT_POLL_CYCLES
			);

		if (this.pollRate <= 0) {
			this.pollRate = ConfigConst.DEFAULT_POLL_CYCLES;
		}

		this.schedExecSvc = Executors.newScheduledThreadPool(1);
		this.sysCpuUtilTask = new SystemCpuUtilTask();
		this.sysMemUtilTask = new SystemMemUtilTask();
		this.sysDiskUtilTask = new SystemDiscUtilTask();

		this.taskRunner = () -> {
			this.handleTelemetry();
		};

		this.locationID =
			ConfigUtil.getInstance().getProperty(
				ConfigConst.GATEWAY_DEVICE,
				ConfigConst.LOCATION_ID_PROP,
				ConfigConst.NOT_SET
			);
	}

	public void handleTelemetry()
	{
		float cpuUtil = this.sysCpuUtilTask.getTelemetryValue();
		float memUtil = this.sysMemUtilTask.getTelemetryValue();
		float diskUtil = this.sysDiskUtilTask.getTelemetryValue();

		_Logger.info(
			"DEBUG sys perf before publish -> cpu=" + cpuUtil +
			", mem=" + memUtil +
			", disk=" + diskUtil
		);

		SystemPerformanceData spd = new SystemPerformanceData();
		spd.setLocationID(this.locationID);
		spd.setCpuUtilization(cpuUtil);
		spd.setMemoryUtilization(memUtil);
		spd.setDiskUtilization(diskUtil);

		_Logger.info("DEBUG SystemPerformanceData object -> " + spd);

		if (this.dataMsgListener != null) {
			this.dataMsgListener.handleSystemPerformanceMessage(
				ResourceNameEnum.GDA_SYSTEM_PERF_MSG_RESOURCE,
				spd
			);
		}
	}

	public void setDataMessageListener(IDataMessageListener listener)
	{
		if (listener != null) {
			this.dataMsgListener = listener;
		}
	}

	public boolean startManager()
	{
		if (!this.isStarted) {
			_Logger.info("SystemPerformanceManager is starting...");

			ScheduledFuture<?> futureTask =
				this.schedExecSvc.scheduleAtFixedRate(
					this.taskRunner,
					1L,
					this.pollRate,
					TimeUnit.SECONDS
				);

			this.isStarted = true;
		} else {
			_Logger.info("SystemPerformanceManager is already started.");
		}

		return this.isStarted;
	}

	public boolean stopManager()
	{
		this.schedExecSvc.shutdown();
		this.isStarted = false;

		_Logger.info("SystemPerformanceManager is stopped.");

		return true;
	}
}