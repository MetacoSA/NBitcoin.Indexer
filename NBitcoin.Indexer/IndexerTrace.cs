using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NBitcoin.Indexer
{
	class IndexerTrace
	{
		static TraceSource _Trace = new TraceSource("NBitcoin.Indexer");
		internal static void ErrorWhileImportingBlockToAzure(uint256 id, Exception ex)
		{
			_Trace.TraceEvent(TraceEventType.Error, 0, "Error while importing " + id + " in azure blob : " + Utils.ExceptionToString(ex));
		}
	}
}
