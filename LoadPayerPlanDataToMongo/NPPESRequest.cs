using System;
using System.IO;
using System.Net;
using System.Text;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;

namespace LoadPayerPlanDataToMongo
{
    internal class StatefulAsyncWebClient
    {
        readonly Action<string> callback;
        public StatefulAsyncWebClient(Action<string> callback, string uri = null)
        {
            this.callback = callback;

            if (uri != null)
                this.uri = uri;
            else
                uri = "https://npiregistry.cms.hhs.gov/api/?number={0}";
        }


        ConcurrentDictionary<int,Task> running = new ConcurrentDictionary<int, Task>();

        readonly string uri;
        public async Task RequestAsync(params string[] values)
        {
            var request = WebRequest.Create(string.Format(uri, values)) as HttpWebRequest;
            var task = request.GetResponseAsync();
            Console.WriteLine("Resuested");
            task.ContinueWith(promise);

            running.TryAdd(task.Id, task);
        }

        public void WaitAll()
        {
            Task.WaitAll(running.Select(x => x.Value).ToArray(), CancellationToken.None);
        }

        private void promise(Task<WebResponse> task)
        {
            var response = (HttpWebResponse)task.Result;
            Console.WriteLine("\t\tReceived");


            if (response.StatusCode == HttpStatusCode.OK)
            {
                var encoding = ASCIIEncoding.ASCII;
                string body = string.Empty;
                using (var reader = new StreamReader(response.GetResponseStream(), encoding))
                {
                    body = reader.ReadToEnd();
                }

                if(body != string.Empty)
                {
                    try
                    {
                        callback(body);
                    }
                    catch(Exception ex)
                    {
                        Console.WriteLine(ex.Message);

                    }
                    finally
                    {
                        Task t;
                        running.TryRemove(task.Id, out t);
                    }

                }
            }
        }
    }
}