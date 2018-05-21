using MongoDB.Bson;
using System.Linq;
using System.Collections.Generic;
using MongoDB.Driver;
using System.IO;
using System.Threading.Tasks;
using System;
using System.Collections.Concurrent;
using System.Threading;

namespace LoadPayerPlanDataToMongo
{
    internal class MongoPayerLoader
    {
        IEnumerable<string> lines;
        ConcurrentQueue<BsonDocument> items;
        public MongoPayerLoader()
        {
            items = new ConcurrentQueue<BsonDocument>();

            lines = File.ReadLines(@"PlanInfoCounty2_flatfiles\PlanInfoCounty_FipsCodeMoreThan30000.csv")
            .Union(File.ReadAllLines(@"PlanInfoCounty2_flatfiles\PlanInfoCounty_FipsCodeLessThan30000.csv").Skip(1));
        }


        public void Load()
        {
            var url = "mongodb://localhost:27017";
            var client = new MongoClient(url);
            var database = client.GetDatabase("US");
            var collection = database.GetCollection<BsonDocument>("PayerPlan");
            Load(lines, collection);
        }

        public async void Load(IEnumerable<string> lines, IMongoCollection<BsonDocument> collection)
        {

            var headers = lines.First().Split(',');
            Task t1  = Queue(lines.Skip(1), headers);
            Task t2  = Load(collection);
            await t1;
            polling = false;
            await t2;
        }


        bool polling = true;

        private async Task Queue(IEnumerable<string> lines, string[] headers)
        {
            var task = Task.Run(() =>
           {
               polling = true;
               int count = 0;
               foreach (var line in lines)
               {
                   try
                   {
                       var cols = line.Split(',');
                       BsonDocument doc = new BsonDocument();
                       var enqueue = false;
                       for (int i = 0; i < headers.Length; i++)
                       {
                           var value = cols[i];
                           if (value != null && value.Trim('\"', ' ') != string.Empty)
                           {
                               doc.Add(headers[i].Trim('\"').Trim(' '), value.Trim('\"', ' '));
                               enqueue = true;
                           }
                       }
                       if (enqueue)
                       {
                           items.Enqueue(doc);
                           count++;
                           if (count == 10000)
                           {
                               Console.WriteLine("Queued 10000 records");
                               count = 0;
                           }

                           if (items.Count > (10 * 100 * 100))
                           {
                               for (int j = 0; j < 5; j++)
                               {
                                   Console.WriteLine("Waiting for insert...");
                                   Thread.Sleep(500);

                               }
                           }
                       }
                   }
                   catch (Exception ex)
                   {
                       Console.WriteLine(ex.ToString());
                       Console.ReadLine();
                       break;
                   }
               }
           });

        }

        private async Task Load(IMongoCollection<BsonDocument> collection)
        {
            while (polling || items.Any())
            {
                try
                {
                    List<BsonDocument> docs = new List<BsonDocument>();
                    for(int i=0;i<10000;i++)
                    {
                        if (items.Any())
                        {
                            BsonDocument b = null;
                            if(items.TryDequeue(out b))
                                docs.Add(b);
                        }
                        else
                            break;
                    }
                    if (docs.Any())
                    {
                        collection.InsertManyAsync(docs);
                        Console.WriteLine("inserting "+docs.Count + "records");
                    }
                }
                catch(Exception ex)
                {
                    Console.WriteLine(ex.ToString());
                    Console.ReadLine();
                }
            }
        }
    }
}