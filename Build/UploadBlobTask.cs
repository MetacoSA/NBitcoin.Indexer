using Microsoft.Build.Framework;
using Microsoft.Build.Utilities;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;


namespace Build
{
	public class UploadBlobTask : Task
	{
		[Required]
		public string Uri
		{
			get;
			set;
		}
		[Required]
		public string AccountName
		{
			get;
			set;
		}
		[Required]
		public string Container
		{
			get;
			set;
		}
		[Required]
		public string BlobName
		{
			get;
			set;
		}
		[Required]
		public string KeyValue
		{
			get;
			set;
		}

		[Required]
		public string File
		{
			get;
			set;
		}
		public override bool Execute()
		{
			CloudBlobClient client = new CloudBlobClient(new StorageUri(new Uri(Uri)), new StorageCredentials(AccountName, KeyValue));
			var container =client.GetContainerReference(Container);
			if(!container.Exists())
			{
				Log.LogError("Container " + Container + " not found");
				return false;
			}
			var blob = container.GetBlockBlobReference(BlobName);
			blob.UploadFromFile(File, System.IO.FileMode.Open);
			blob.Properties.CacheControl = "no-cache";
			blob.Properties.ContentType = "application/octet-stream";
			blob.Properties.ContentDisposition = "attachment; filename=\""+ Path.GetFileName(File) +"\"";
			blob.SetProperties();
			return true;
		}
	}
}
