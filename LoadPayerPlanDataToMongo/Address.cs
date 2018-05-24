using System;
using System.Linq;
using System.Collections;
using MongoDB.Bson;
using MongoDB.Driver;

namespace LoadPayerPlanDataToMongo
{
    internal class Address
    {

        public string Zip { get; private set; }
        public string State { get; private set; }
        public string City { get; private set; }

        public string AddressLine1 { get; private set; }
        public string AddressLine2 { get; private set; }


        private string[] parts;
        private IMongoCollection<BsonDocument> zipCodeStates;

        public Address(BsonDocument b, IMongoCollection<BsonDocument> x)
        {
            this.zipCodeStates = x;

            this.parts = getAddress(b);
            setupAddress();
        }

        private void setupAddress()
        {
            var enumParts = from part in parts
                            select part.Trim(' ');
            var address = string.Join(" ", enumParts);

            var lexemes = address.Split(' ');

            foreach (var lexeme in lexemes)
            {
                if (IsZip(lexeme) && Zip == null)
                    Zip = lexeme;

                else if (IsState(lexeme) && State == null)
                    State = lexeme;

                else if (IsCity(lexeme) && City == null)
                    City = lexeme;

                else if (string.IsNullOrEmpty(AddressLine1))
                    AddressLine1 = lexeme.Trim(' ').Trim('\"');

                else if (string.IsNullOrEmpty(AddressLine2))
                    AddressLine2 = lexeme.Trim(' ').Trim('\"');
            }
            if(Zip != null)
            {
                var addr = zipCodeStates.Find("{\"Zip Code\" : '" + Zip + "'}");
                var first = addr.First();
                if(first !=null)
                {
                    State = first["State Abbreviation"].AsString;
                    City = first["Place Name"].AsString;
                }
                

            }
        }

        private bool IsCity(string lexeme)
        {
            lexeme = lexeme.Trim().Trim('\"');

            if (string.IsNullOrEmpty(lexeme))
                return false;

            var results = zipCodeStates.Find("{\"Place Name\" : '" + lexeme + "'}");
            if (results.Any())
                return true;

            results = zipCodeStates.Find("{County : '" + lexeme + "'}");
            if (results.Any())
                return true;

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

            var results = zipCodeStates.Find("{\"Zip Code\" : '" + lexeme + "'}");
            if (!results.Any())
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

            var results = zipCodeStates.Find("{\"State Abbreviation\" : '" + lexeme + "'}");
            if (!results.Any())
                return false;

            return true;
        }

        public string[] getAddress(BsonDocument b)
        {
            var fl = get("First_Line_BizAdd", b) ?? get("First_Line_BizLocAdd", b);

            var sl = get("Second_Line_BizAdd", b) ?? get("Second_Line_BizLocAdd", b);

            var ci = get("BizAdd_City_Name", b) ?? get("BizLocAdd_City_Name", b);

            var st = get("BizAdd_State_Name", b) ?? get("BizLocAdd_State_Name", b);

            var ot = get("BizLocAdd_Country_Code_If_outside_US", b);

            var po = getPO(b);
            return new[] { fl, sl, ci, st, po, ot };
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