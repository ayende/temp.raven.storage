// -----------------------------------------------------------------------
//  <copyright file="RecoveryTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using Xunit;

namespace Raven.Storage.Tests.Recovery
{
	public class RecoveryTests : StorageTestBase
	{
		[Fact]
		public void ShouldRecoverDataFromLogFile()
		{
			var storage = NewStorage();

			var name = storage.Name;

			var writeBatch = new WriteBatch();
			var aValue = new byte[] {1, 2, 3};
			writeBatch.Put("A", new MemoryStream(aValue));
			var bValue = new byte[] {1, 2};
			writeBatch.Put("B", new MemoryStream(bValue));
			storage.Writer.Write(writeBatch);

			storage.Dispose();

			using (var newStorage = new Storage(name, new StorageOptions()))
			{
				var aData = ((MemoryStream)newStorage.Reader.Read("A")).ToArray();
				var bData = ((MemoryStream)newStorage.Reader.Read("B")).ToArray();

				for (int i = 0; i < aValue.Length; i++)
				{
					Assert.Equal(aValue[i], aData[i]);
				}

				for (int i = 0; i < bValue.Length; i++)
				{
					Assert.Equal(bValue[i], bData[i]);
				}
			}
		}
	}
}