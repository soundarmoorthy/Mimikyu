using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace LoadPayerPlanDataToMongo
{
    public class NPPESLoader
    {
        public NPPESLoader()
        {

        }


        IMongoDatabase us;
        IMongoCollection<BsonDocument> nppes;
        StatefulAsyncWebClient nppesRequests;

        public async void Run()
        {
            var url = "mongodb://localhost:27017";
            var client = new MongoClient(url);
            var database = client.GetDatabase("NPPEES");
            var collection = database.GetCollection<BsonDocument>("US_Provider_Roaster");

            us = client.GetDatabase("US");
            nppes = us.GetCollection<BsonDocument>("NPPES");

            nppesRequests = new StatefulAsyncWebClient(process);

            await Queue(collection);
            awaitAllPending();
            Console.Read();
        }

        private void awaitAllPending()
        {
            nppesRequests.WaitAll();
        }

        private async Task Queue(IMongoCollection<BsonDocument> collection)
        {
            var t = collection.Find("{Entity_Type_Code : '2'}").ForEachAsync(b =>
            {
                var id = get("_id", b);
                GetAndPersistAsync(id);
            });

            t.Wait();
        }

        private  void GetAndPersistAsync(string npi)
        {
            var task = nppesRequests.RequestAsync(npi);
            Console.WriteLine("Requested");
        }

        private void process(string json)
        {
            var document = BsonSerializer.Deserialize<BsonDocument>(json);
            nppes.InsertOne(document);
            Console.WriteLine("\t\tInserted");
        }

        private static string get(string name, BsonDocument b)
        {
            string item = "";
            if (b.Contains(name))
                item = b.GetValue(name).AsString.Trim();

            return item;
        }
    }
}