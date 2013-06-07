// -----------------------------------------------------------------------
//  <copyright file="ReadLogResult.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Storage.Reading
{
	public class LogReadResult
	{
		public ulong WriteSequence { get; set; }
		public WriteBatch WriteBatch { get; set; }
	}
}