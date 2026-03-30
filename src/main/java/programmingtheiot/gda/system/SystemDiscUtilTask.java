package programmingtheiot.gda.system;

import java.util.logging.Logger;

import programmingtheiot.common.ConfigConst;

public class SystemDiscUtilTask extends BaseSystemUtilTask
{
	private static final Logger _Logger =
		Logger.getLogger(SystemDiscUtilTask.class.getName());

	public SystemDiscUtilTask()
	{
		super(ConfigConst.NOT_SET, ConfigConst.DEFAULT_TYPE_ID);
	}

	@Override
	public float getTelemetryValue()
	{
		java.io.File f = new java.io.File("/");

		double total = (double) f.getTotalSpace();
		double free  = (double) f.getFreeSpace();

		if (total <= 0.0d) {
			_Logger.warning("Disk total space is 0 or unavailable for path: /");
			return 0.0f;
		}

		double used = total - free;
		double diskUtil = (used / total) * 100.0d;

		_Logger.info(
			"DEBUG disk telemetry -> total=" + total +
			", free=" + free +
			", used=" + used +
			", diskUtil=" + diskUtil
		);

		return (float) diskUtil;
	}
}