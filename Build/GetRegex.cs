using Microsoft.Build.Framework;
using Microsoft.Build.Utilities;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace Build
{
	public class GetRegex : Task
	{
		[Required]
		public string File
		{
			get;
			set;
		}
		[Required]
		public string Regex
		{
			get;
			set;
		}
		public int Group
		{
			get;
			set;
		}

		[Output]
		public string Result
		{
			get;
			set;
		}

		public override bool Execute()
		{
			var file = System.IO.File.ReadAllText(File);
			var match = System.Text.RegularExpressions.Regex.Match(file, Regex);
			Result = match.Groups[Group].Value;
			return true;
		}
	}

	public class SetRegex : Task
	{
		[Required]
		public string File
		{
			get;
			set;
		}
		[Required]
		public string Regex
		{
			get;
			set;
		}
		[Required]
		public string Replacement
		{
			get;
			set;
		}


		public override bool Execute()
		{
			var file = System.IO.File.ReadAllText(File);
			var result = System.Text.RegularExpressions.Regex.Replace(file, Regex, Replacement);
			System.IO.File.WriteAllText(File, result);
			return true;
		}
	}
}
