using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using Newtonsoft.Json.Linq;
using MongoDB.Driver;

namespace LoadPayerPlanDataToMongo.Patient
{
    class PatientParseStrategy2 : IParseStrategy
    {

        private class SlashRContainsComparer : IEqualityComparer<string>
        {
            public bool Equals(string x, string y)
            {
                x = x.ToLower().Trim('\r').Trim(' ');
                y = y.ToLower().Trim('\r').Trim(' ');

                return x.Contains(y) || y.Contains(x);

            }

            public int GetHashCode(string obj)
            {
                return obj.GetHashCode();
            }
        }


        private static Dictionary<string, string> stateCityMap = new Dictionary<string,string>(new SlashRContainsComparer());

        private static Dictionary<string, string> cityZipMap = new Dictionary<string,string>(new SlashRContainsComparer());
        public PatientParseStrategy2()
        {
            if (!cached)
            {
                cached = true;
                initializeCache();
            }
        }

        private static void initializeCache()
        {
            var url = "mongodb://localhost:27017";
            var client = new MongoClient(url);
            var database = client.GetDatabase("NPPEES");
            var collection = database.GetCollection<BsonDocument>("ZipCodeTimeZone");
            var task = collection.Find("{}").ForEachAsync(b =>
            {
                var zip = b.GetValue("zip").ToString().Trim('\r');
                var state = b.GetValue("state").ToString().Trim('\r');
                var city = b.GetValue("city").ToString().Trim('\r');
                if (!stateCityMap.ContainsKey(state))
                    stateCityMap.Add(state, city);

                if (!cityZipMap.ContainsKey(city))
                    cityZipMap.Add(city, zip);

            });
            task.Wait();
        }

        static bool cached = false;

        public Patient Parse(BsonDocument b)
        {
            Patient p = new Patient();
            p.Id = b.GetValue("_id").ToString();
            b.Remove("_id");
            var json = JObject.Parse(b.ToString());

            p.AddressLine1 = json["Addr1"].ToString();
            p.AddressLine2 = json["Addr2"].ToString();
            p.City = json["City"].ToString().Trim('\r');
            p.State = json["State\r"]?.ToString().Trim('\r') ?? json["State"].ToString();

            try
            {
                p.Zipcode = FindZip(p.City, p.State);
            }

            finally
            {

            }

            //if (a4 == "PR" || a4 == "VI")
            //    return "PR not valid state";

            p.Firstname = json["FirstName"].ToString();
            p.Lastname = json["LastName"].ToString();
            p.DateOfBirth = json["DateOfBirth"].ToString();
            p.Phone = json["Phone"].ToString();
            p.Gender = json["Gender"].ToString();

            return p;
        }

        private string FindZip(string city, string state)
        {
            return cityZipMap[stateCityMap[state]];

        }
    }
}
