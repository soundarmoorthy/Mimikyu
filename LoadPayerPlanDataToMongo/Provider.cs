using MongoDB.Bson;
using Newtonsoft.Json.Linq;
using System.Linq;
using System;

namespace LoadPayerPlanDataToMongo
{
    internal class Provider
    {

        private readonly JObject source;
        int I = 0;

        public Provider(BsonDocument b)
        {
            Id = b.GetElement("_id").Value.ToString();
            b.Remove("_id"); //This part doesn't count as valid json. So we remove it, so that parsing works

            this.source = JObject.Parse(b.ToString());
        }


        public Provider Location() => Context(0);

        public Provider Mailing() => Context(1);


        JToken address => source["addresses"][I];


        public readonly string Id;

        public string NPI => source["number"].ToString();

        public string OrgName => Canonical(source["basic"]["organization_name"]);


        public string AddrLine1 => Canonical(address["address_1"]);

        public string AddrLine2 => Canonical(address["address_2"]);


        public string City => Canonical(address["city"]);

        public string StateCode => Canonical(address["state"]);


        public string CountryCode => Canonical(address["country_code"]);


        public string Description => Canonical(source["taxonomies"][0]["desc"]);

        public string Phone => ParseOrForce(address["telephone_number"]);


        public string Fax => ParseOrForce(address["fax_number"]);

        public string ZipCode
        {
            get
            {
                var pin = Canonical(address["postal_code"]);
                return pin.Length > 5 ? pin.Substring(0, 5) : pin;
            }
        }



        private string Canonical(JToken token)
        {
            if (token == null)
                return null;
            else
                return token.ToString().Replace("'", "");
        }

        private string ParseOrForce(JToken token)
        {
            if (token == null)
                return null;

            var str = from ch in token.ToString()
                      where char.IsNumber(ch)
                      select ch;


            return str.Count() == 10
                ? string.Join("", str)
                : Force();//A rare possiblity of collision
        }

        private static string Force()
        {
            return new Random().Next(1000000000, int.MaxValue).ToString();
        }

        private Provider Context(int i)
        {
            I = i;
            return this;
        }

    }
}