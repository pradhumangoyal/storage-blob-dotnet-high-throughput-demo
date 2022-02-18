//------------------------------------------------------------------------------
//MIT License

//Copyright(c) 2019 Microsoft Corporation. All rights reserved.

//Permission is hereby granted, free of charge, to any person obtaining a copy
//of this software and associated documentation files (the "Software"), to deal
//in the Software without restriction, including without limitation the rights
//to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//copies of the Software, and to permit persons to whom the Software is
//furnished to do so, subject to the following conditions:

//The above copyright notice and this permission notice shall be included in all
//copies or substantial portions of the Software.

//THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
//SOFTWARE.
//------------------------------------------------------------------------------

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Sample_HighThroughputBlobUpload
{
    /// <summary>
    /// This class is responsible for orchestrating the append to a blob across one-to-many workers.
    /// </summary>
    public class AppendTestRunner : UploadTestRunner
    {
        public AppendTestRunner(CloudStorageAccount storageAccount) :
            base(storageAccount)
        {

        }

        public override async Task Run(string[] args)
        {
            // Parse the arguments.
            if (!ParseArguments(args, out uint blockSizeBytes, out uint totalBlocks, out uint numInstances, out string blobName, out string containerName))
            {
                // If invalid arguments were provided, exit the test.
                return;
            }

            CloudBlobClient blobClient = StorageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);
            CloudAppendBlob appendBlob = container.GetAppendBlobReference(blobName);
            await container.CreateIfNotExistsAsync();
            // Create append blob
            await appendBlob.CreateOrReplaceAsync();

            uint numBlocksPerPass = totalBlocks / numInstances;
            uint remainder = totalBlocks % numInstances;

            Task[] tasks = new Task[numInstances];
            HashSet<Guid> operationIDs = new HashSet<Guid>();

            // Assign the Work
            for (uint i = 0; i < numInstances; ++i)
            {
                uint numBlocks = numBlocksPerPass;
                // Evenly distribute remaining blocks across instances such that no instance differs by more than 1 block.
                if (remainder > 0)
                {
                    numBlocks++;
                    remainder--;
                }

                Guid opId = Guid.NewGuid();
                operationIDs.Add(opId);

                AppendBlockOperation operation = new AppendBlockOperation(opId, blobName, containerName, blockSizeBytes, numBlocks);
                TestMsg appendBlockMsg = new TestMsg(Environment.MachineName, operation);
                tasks[i] = JobQueue.AddMessageAsync(CreateJobQueueMessage(appendBlockMsg));
            }

            await Task.WhenAll(tasks);

            // Report timing status
            await ReportStatus(operationIDs, ((long)totalBlocks) * blockSizeBytes);
        }
    }
}
