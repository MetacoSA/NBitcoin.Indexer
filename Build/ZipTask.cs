using Microsoft.Build.Framework;
using Microsoft.Build.Utilities;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace Build
{
    public class ZipTask : Task
    {
        public string OutputFile
        {
            get;
            set;
        }
        public string Folder
        {
            get;
            set;
        }
        public override bool Execute()
        {
            if (File.Exists(OutputFile))
                File.Delete(OutputFile);
            ZipFile.CreateFromDirectory(Folder, OutputFile);
            return true;
        }
    }
}
