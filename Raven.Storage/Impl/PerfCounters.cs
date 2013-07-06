using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Security;
using Raven.Temp.Logging;

namespace Raven.Storage.Impl
{
	public class PerfCounters : IDisposable
	{
		private static readonly ILog log = LogManager.GetCurrentClassLogger();

		private PerformanceCounter _writesPerSecCounter;
		private PerformanceCounter _readsPerSecCounter;
		private PerformanceCounter _bytesWrittenPerSecCounter;
		private PerformanceCounter _bytesReadPerSecCounter;
		private readonly bool _useCounters;

		public void BytesRead(long size)
		{
			if (_useCounters)
			{
				_bytesReadPerSecCounter.IncrementBy(size);
			}
		}

		public void BytesWritten(long size)
		{
			if (_useCounters)
			{
				_bytesWrittenPerSecCounter.IncrementBy(size);
			}
		}

		public void Write(int count)
		{
			if (_useCounters)
			{
				_writesPerSecCounter.IncrementBy(count);
			}
		}

		public void Read()
		{
			if (_useCounters)
			{
				_readsPerSecCounter.Increment();
			}
		}

		public PerfCounters(string name)
		{
			try
			{
				SetupPerformanceCounter(GetPerformanceCounterName(name));
				_useCounters = true;
			}
			catch (UnauthorizedAccessException e)
			{
				log.WarnException(
					"Could not setup performance counters properly because of access permissions, perf counters will not be used", e);
				_useCounters = false;
			}
			catch (SecurityException e)
			{
				log.WarnException(
					"Could not setup performance counters properly because of access permissions, perf counters will not be used", e);
				_useCounters = false;
			}
		}

		private string GetPerformanceCounterName(string name)
		{
			if (string.IsNullOrEmpty(name))
				return "null";
			//dealing with names who are very long (there is a limit of 80 chars for counter name)
			return name.Length > 70 ? name.Remove(70) : name;
		}

		private void SetupPerformanceCounter(string name)
		{
			const string categoryName = "Raven.Storage";
			var instances = new Dictionary<string, PerformanceCounterType>
			{
				{"# writes / sec", PerformanceCounterType.RateOfCountsPerSecond32},
				{"# reads / sec", PerformanceCounterType.RateOfCountsPerSecond32}, 
				{"bytes written / sec", PerformanceCounterType.RateOfCountsPerSecond64},
				{"bytes read / sec", PerformanceCounterType.RateOfCountsPerSecond64}, 
			};

			if (IsValidCategory(categoryName, instances, name) == false)
			{
				var counterCreationDataCollection = new CounterCreationDataCollection();
				foreach (var instance in instances)
				{
					counterCreationDataCollection.Add(new CounterCreationData
					{
						CounterName = instance.Key,
						CounterType = instance.Value
					});
				}

				PerformanceCounterCategory.Create(categoryName, "Raven.Storage Performance Counters", PerformanceCounterCategoryType.MultiInstance, counterCreationDataCollection);
				PerformanceCounter.CloseSharedResources(); // http://blog.dezfowler.com/2007/08/net-performance-counter-problems.html
			}

			_writesPerSecCounter = new PerformanceCounter(categoryName, "# writes / sec", name, false);
			_readsPerSecCounter = new PerformanceCounter(categoryName, "# reads / sec", name, false);
			_bytesWrittenPerSecCounter = new PerformanceCounter(categoryName, "bytes written / sec", name, false);
			_bytesReadPerSecCounter = new PerformanceCounter(categoryName, "bytes read / sec", name, false);
		}

		private bool IsValidCategory(string categoryName, Dictionary<string, PerformanceCounterType> instances, string instanceName)
		{
			if (PerformanceCounterCategory.Exists(categoryName) == false)
				return false;
			foreach (var performanceCounterType in instances)
			{
				try
				{
					new PerformanceCounter(categoryName, performanceCounterType.Key, instanceName, readOnly: false).Dispose();
				}
				catch (Exception)
				{
					PerformanceCounterCategory.Delete(categoryName);
					return false;
				}
			}
			return true;
		}

		public void Dispose()
		{
			if (_useCounters == false)
				return;
			_writesPerSecCounter.Dispose();
			_readsPerSecCounter.Dispose();
			_bytesWrittenPerSecCounter.Dispose();
			_bytesReadPerSecCounter.Dispose();
		}
	}
}