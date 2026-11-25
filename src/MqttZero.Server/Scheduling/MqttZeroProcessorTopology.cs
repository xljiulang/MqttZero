using System.Runtime.InteropServices;

namespace MqttZero.Server.Scheduling;

internal sealed class MqttZeroProcessorTopology :
    IMqttZeroProcessorTopologyProbe
{
    private const int RelationProcessorCore = 0;

    public int GetPhysicalCoreCount()
    {
        try
        {
            if (OperatingSystem.IsWindows())
            {
                return GetWindowsPhysicalCoreCount();
            }

            if (OperatingSystem.IsLinux())
            {
                return GetLinuxPhysicalCoreCount();
            }
        }
        catch (Exception)
        {
        }

        return Environment.ProcessorCount;
    }

    private static int GetWindowsPhysicalCoreCount()
    {
        uint byteLength = 0;
        _ = GetLogicalProcessorInformationEx(
            RelationProcessorCore,
            IntPtr.Zero,
            ref byteLength);
        if (byteLength == 0)
        {
            return Environment.ProcessorCount;
        }

        IntPtr buffer = Marshal.AllocHGlobal(checked((int)byteLength));
        try
        {
            if (!GetLogicalProcessorInformationEx(
                    RelationProcessorCore,
                    buffer,
                    ref byteLength))
            {
                return Environment.ProcessorCount;
            }

            int coreCount = 0;
            int offset = 0;
            while (offset + sizeof(int) * 2 <= byteLength)
            {
                int relationship = Marshal.ReadInt32(buffer, offset);
                int entrySize = Marshal.ReadInt32(
                    buffer,
                    offset + sizeof(int));
                if (entrySize <= 0)
                {
                    break;
                }

                if (relationship == RelationProcessorCore)
                {
                    coreCount++;
                }

                offset += entrySize;
            }

            return coreCount > 0
                ? coreCount
                : Environment.ProcessorCount;
        }
        finally
        {
            Marshal.FreeHGlobal(buffer);
        }
    }

    private static int GetLinuxPhysicalCoreCount()
    {
        const string cpuPath = "/sys/devices/system/cpu";
        if (!Directory.Exists(cpuPath))
        {
            return Environment.ProcessorCount;
        }

        HashSet<string> cores = new(StringComparer.Ordinal);
        foreach (string cpuDirectory in Directory.EnumerateDirectories(
            cpuPath,
            "cpu[0-9]*",
            SearchOption.TopDirectoryOnly))
        {
            string topologyPath = Path.Combine(
                cpuDirectory,
                "topology");
            string packagePath = Path.Combine(
                topologyPath,
                "physical_package_id");
            string corePath = Path.Combine(
                topologyPath,
                "core_id");
            if (!File.Exists(packagePath) || !File.Exists(corePath))
            {
                continue;
            }

            string package = File.ReadAllText(packagePath).Trim();
            string core = File.ReadAllText(corePath).Trim();
            cores.Add(string.Concat(package, ":", core));
        }

        return cores.Count > 0
            ? cores.Count
            : Environment.ProcessorCount;
    }

#pragma warning disable SYSLIB1054
    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool GetLogicalProcessorInformationEx(
        int relationshipType,
        IntPtr buffer,
        ref uint returnedLength);
#pragma warning restore SYSLIB1054
}
