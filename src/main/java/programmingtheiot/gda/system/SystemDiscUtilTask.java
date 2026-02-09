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

package programmingtheiot.gda.system;

import java.util.logging.Logger;

import programmingtheiot.common.ConfigConst;

/**
 * Shell representation of class for student implementation.
 * 
 */
public class SystemDiscUtilTask extends BaseSystemUtilTask
{
	// constructors
	private static final Logger _Logger =
		Logger.getLogger(SystemDiscUtilTask.class.getName());
	/**
	 * Default.
	 * 
	 */
	public SystemDiscUtilTask()
	{
		super(ConfigConst.NOT_SET, ConfigConst.DEFAULT_TYPE_ID);
	}
	
	
	// public methods
	
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

		_Logger.fine("Disk used: " + used + "; Disk total: " + total + "; Disk util(%): " + diskUtil);

		return (float) diskUtil;
	}
}