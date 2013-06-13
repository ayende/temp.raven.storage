namespace Raven.Storage.Benchmark
{
	using System;
	using System.Text;

	using Raven.Storage.Benchmark.Env;

	internal class Histogram
	{
		private const int NumberOfBuckets = 154;

		private readonly double[] bucketLimit = new double[NumberOfBuckets]
			                                        {
				                                        1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 18, 20, 25, 30, 35, 40, 45, 50,
				                                        60, 70, 80, 90, 100, 120, 140, 160, 180, 200, 250, 300, 350, 400, 450, 500,
				                                        600, 700, 800, 900, 1000, 1200, 1400, 1600, 1800, 2000, 2500, 3000, 3500,
				                                        4000, 4500, 5000, 6000, 7000, 8000, 9000, 10000, 12000, 14000, 16000, 18000
				                                        , 20000, 25000, 30000, 35000, 40000, 45000, 50000, 60000, 70000, 80000,
				                                        90000, 100000, 120000, 140000, 160000, 180000, 200000, 250000, 300000,
				                                        350000, 400000, 450000, 500000, 600000, 700000, 800000, 900000, 1000000,
				                                        1200000, 1400000, 1600000, 1800000, 2000000, 2500000, 3000000, 3500000,
				                                        4000000, 4500000, 5000000, 6000000, 7000000, 8000000, 9000000, 10000000,
				                                        12000000, 14000000, 16000000, 18000000, 20000000, 25000000, 30000000,
				                                        35000000, 40000000, 45000000, 50000000, 60000000, 70000000, 80000000,
				                                        90000000, 100000000, 120000000, 140000000, 160000000, 180000000, 200000000,
				                                        250000000, 300000000, 350000000, 400000000, 450000000, 500000000, 600000000
				                                        , 700000000, 800000000, 900000000, 1000000000, 1200000000, 1400000000,
				                                        1600000000, 1800000000, 2000000000, 2500000000.0, 3000000000.0,
				                                        3500000000.0, 4000000000.0, 4500000000.0, 5000000000.0, 6000000000.0,
				                                        7000000000.0, 8000000000.0, 9000000000.0, 1e200
			                                        };

		private double min;

		private double max;

		private int num;

		private double sum;

		private double sumSquares;

		private readonly double[] buckets;

		public Histogram()
		{
			min = bucketLimit[NumberOfBuckets - 1];
			max = 0;
			num = 0;
			sum = 0;
			sumSquares = 0;
			buckets = new double[NumberOfBuckets];

			for (var i = 0; i < NumberOfBuckets; i++)
				buckets[i] = 0;
		}

		public void Add(double value)
		{
			// Linear search is fast enough for our usage in db_bench
			int b = 0;
			while (b < NumberOfBuckets - 1 && bucketLimit[b] <= value)
			{
				b++;
			}

			buckets[b] += 1.0;
			if (min > value) min = value;
			if (max < value) max = value;
			num++;
			sum += value;
			sumSquares += (value * value);
		}

		public void Merge(Histogram other)
		{
			if (other.min < min) min = other.min;
			if (other.max > max) max = other.max;
			num += other.num;
			sum += other.sum;
			sumSquares += other.sumSquares;
			for (var b = 0; b < NumberOfBuckets; b++)
			{
				buckets[b] += other.buckets[b];
			}
		}

		private double Median()
		{
			return Percentile(50.0);
		}

		private double Percentile(double p)
		{
			double threshold = num * (p / 100.0);
			double localSum = 0;
			for (var b = 0; b < NumberOfBuckets; b++)
			{
				localSum += buckets[b];
				if (!(localSum >= threshold))
					continue;

				// Scale linearly within this bucket
				var leftPoint = (b == 0) ? 0 : bucketLimit[b - 1];
				var rightPoint = bucketLimit[b];
				var leftSum = localSum - buckets[b];
				var rightSum = localSum;
				var pos = (threshold - leftSum) / (rightSum - leftSum);
				var r = leftPoint + (rightPoint - leftPoint) * pos;
				if (r < min) r = min;
				if (r > max) r = max;

				return r;
			}

			return max;
		}

		private double Average()
		{
			if (num == 0)
				return 0;

			return sum / num;
		}

		private double StandardDeviation()
		{
			if (num == 0)
				return 0;

			var variance = (sumSquares * num - sum * sum) / (num * num);

			return Math.Sqrt(variance);
		}

		public override string ToString()
		{
			var builder = new StringBuilder();
			builder.AppendLine(string.Format("Count: {0}	Average: {1:0000}	StdDev: {2:00}", num, Average(), StandardDeviation()));
			builder.AppendLine(string.Format("Min: {0:0000}	Median: {1:0000}	Max: {2:0000}", num == 0 ? 0 : min, Median(), max));
			builder.AppendLine(Constants.Separator);
			builder.AppendLine("[ Min, Max )	Count		%		Cumulative %");

			var mult = 100.0 / num;
			var localSum = 0.0;
			for (var b = 0; b < NumberOfBuckets; b++)
			{
				if (buckets[b] <= 0.0)
					continue;

				localSum += buckets[b];
				builder.Append(
					string.Format(
						"[ {0}, {1} )	{2}		{3:000}		{4:000} ",
						b == 0 ? 0.0 : bucketLimit[b - 1],	// left
						bucketLimit[b],						// right
						buckets[b],							// count
						mult * buckets[b],					// percentage
						mult * localSum));					// cumulative percentage

				// Add hash marks based on percentage; 20 marks for 100%.
				var marks = (int)(20 * (buckets[b] / num) + 0.5);
				for (var i = 0; i < marks; i++)
				{
					builder.Append('#');
				}

				builder.AppendLine();
			}

			return builder.ToString();
		}
	}
}