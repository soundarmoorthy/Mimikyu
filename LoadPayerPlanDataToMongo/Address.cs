using System;
using System.Linq;
using System.Collections;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Collections.Generic;

namespace LoadPayerPlanDataToMongo
{
    internal class Address
    {

        public string Zip { get; private set; }
        public string State { get; private set; }
        public string City { get; private set; }

        public string AddressLine1 { get; private set; }
        public string AddressLine2 { get; private set; }

        public string NPI { get; private set; }


        public Address(BsonDocument b, IMongoCollection<BsonDocument> x)
        {
            if (cache == null)
                setupLookup(x);

            formAddress(b);
            NPI = get("_id", b);
        }

        private void setupLookup(IMongoCollection<BsonDocument> zipCodeStates)
        {
            var task = zipCodeStates.Find("{}").ToListAsync();
            task.Wait();
            cache = task.Result;
            cache.RemoveAll(x => x.Any(y => y.Value == BsonNull.Value));
        }

        static List<BsonDocument> cache;


        private bool IsCity(string lexeme)
        {
            lexeme = lexeme.Trim().Trim('\"').Trim('\"').ToLower();

            if (string.IsNullOrEmpty(lexeme))
                return false;

            var result = cache.FirstOrDefault(x => 
                {
                    var data = x["Place Name"];
                    var data1 = data.AsString;
                    var data2 = data1.Trim(' ').ToLower();
                    return data2 == lexeme;
            });
            if (result != null)
            {
                return true;
            }

            result = cache.FirstOrDefault(x => x["County"]?.AsString.Trim(' ').ToLower() == lexeme);
            if (result != null)
            {
                return true;
            }

            return false;

        }

        private bool IsZip(string lexeme)
        {
            lexeme = lexeme.Trim().Trim('\"');
            if (!lexeme.All(x => char.IsNumber(x)))
                return false;

            if (lexeme.Length == 9)
                lexeme = lexeme.Substring(0, 5);

            else if (lexeme.Length != 5)
                return false;

            var result = cache.FirstOrDefault(x => x["Zip Code"].AsString.Trim(' ') == lexeme);
            if (result == null)
                return false;

            return true;

        }

        private bool IsState(string lexeme)
        {
            lexeme = lexeme.Trim().Trim('\"');

            if (lexeme.Length != 2)
                return false;

            if (!lexeme.All(x => char.IsLetter(x)))
                return false;

            var result = cache.FirstOrDefault(x => x["State Abbreviation"].AsString.Trim(' ') == lexeme);
            if ( result == null)
                return false;

            return true;
        }

        public void formAddress(BsonDocument b)
        {
            var fl = get("First_Line_BizAdd", b) ?? get("First_Line_BizLocAdd", b);
            var sl = get("Second_Line_BizAdd", b) ?? get("Second_Line_BizLocAdd", b);
            var ci = get("BizAdd_City_Name", b) ?? get("BizLocAdd_City_Name", b);
            var st = get("BizAdd_State_Name", b) ?? get("BizLocAdd_State_Name", b);
            var ot = get("BizLocAdd_Country_Code_If_outside_US", b);

            var po = getPO(b);
            if (IsZip(po) && Zip == null)
                Zip = po.Length == 9 ? po.Substring(0, 5) : po;

            else if (IsState(st) && State == null)
                State = st;

            else if (IsCity(ci) && City == null)
                City = ci;

            else if (string.IsNullOrEmpty(AddressLine1))
                AddressLine1 = fl?.Trim(' ').Trim('\"') ?? "";

            else if (string.IsNullOrEmpty(AddressLine2))
                AddressLine2 = sl?.Trim(' ').Trim('\"') ?? "";

            if (Zip != null)
            {

                var first = cache.FirstOrDefault(x => x["Zip Code"].AsString.Trim(' ') == Zip);
                if (first != null)
                {
                    State = first["State Abbreviation"].AsString;
                    City = first["Place Name"].AsString;
                }
                else
                {
                    Console.WriteLine("oh god");
                }
            }
        }

        private string getPO(BsonDocument b)
        {

            string[] fields = { "BizAdd_Postal_Code", "BizLocAdd_Postal_Code", "BizAdd_Country_Code_If_outside_US" };
            int po = 0;

            foreach (var field in fields)
            {

                var value = get(field,b);
                var parsed = int.TryParse(value, out po);
                if (parsed)
                    return value.ToString().Substring(0, 5);
            }
            return null;
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