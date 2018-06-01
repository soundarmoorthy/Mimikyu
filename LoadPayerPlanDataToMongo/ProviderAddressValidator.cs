using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LoadPayerPlanDataToMongo

{
    public class ProviderAddressValidator
    {
        Dictionary<string, int> codes = new Dictionary<string, int>();

        IMongoCollection<BsonDocument> zipCodeStates;

        ConcurrentQueue<BsonDocument> doc = new ConcurrentQueue<BsonDocument>();

        public void Run()
        {
            var url = "mongodb://localhost:27017";
            var client = new MongoClient(url);
            var database = client.GetDatabase("NPPEES");
            var collection = database.GetCollection<BsonDocument>("US_Provider_Roaster");
            zipCodeStates = database.GetCollection<BsonDocument>("ZipCodeStateNameMap");

            var count = 0;

            //RunValidation1();
            //RunValidation2();
            //RunValidation3();
            Task t = RunValidation();
            polling = true;
            var action = collection.Find("{Entity_Type_Code : '2'}").ForEachAsync(b =>
            {
                doc.Enqueue(b);
            });

            action.Wait();
            polling = false;
            t.Wait();
            var value = string.Join(Environment.NewLine, invalidZips.Union(exception));
            File.WriteAllText("c:\\cmr\\invalidNPI.txt", value);

            Console.Read();
        }

        bool polling = false;

        private async Task RunValidation()
        {
            var task = Task.Run(() =>
            {
                int count = 0;
                string result = "";
                while (polling || doc.Any())
                {
                    try
                    {
                        BsonDocument b = null;

                        if (doc.TryDequeue(out b))
                        {
                            var address = new Address(b, zipCodeStates);

                            if (address.Zip == null)
                            {
                                invalidZips.Add(address.NPI);
                                result = "Invalid Zip";
                            }
                            else
                                result = "Verified Zip";
                        }
                    }
                    catch (Exception ex)
                    {
                        exception.Add(ex.Message);
                        result = "Exception";
                    }

                    if (codes.ContainsKey(result))
                    {
                        codes[result]++;
                    }
                    else
                    {
                        codes.Add(result, 1);
                    }
                    count++;


                    if (count == 10000  || count > 0)
                    {
                        Console.Clear();
                        Console.CursorLeft = 0;
                        Console.CursorTop = 0;
                        foreach (var item in codes.Keys)
                        {
                            Console.WriteLine(item + "-" + codes[item]);

                        }
                        Console.WriteLine(doc.Count);
                        count = 0;
                    }
                }
                //return "OK";
            });
        }

        List<string> invalidZips = new List<string>();
        List<string> exception = new List<string>();
    }
}
