using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LoadPayerPlanDataToMongo

{
    public class MongoToSqlServerPracticeLoader
    {
        Dictionary<string, int> codes = new Dictionary<string, int>();

        public void Load()
        {
            var url = "mongodb://localhost:27017";
            var client = new MongoClient(url);
            var database = client.GetDatabase("NPPEES");
            var collection = database.GetCollection<BsonDocument>("US_Provider_Roaster");
            var zipCodeTimeZones = database.GetCollection<BsonDocument>("ZipCodeTimeZone");
            var zipCodeStates = database.GetCollection<BsonDocument>("ZipCodeStateNameMap");


            using (SqlConnection conn = new SqlConnection("Data Source=.;Initial Catalog=CMR;Integrated Security=True"))
            {
                conn.Open();
                Run(collection, zipCodeTimeZones, zipCodeStates, conn);
            }

        }

        private void Run(IMongoCollection<BsonDocument> collection, 
            IMongoCollection<BsonDocument> zipCodeTimeZones, 
            IMongoCollection<BsonDocument> zipCodeStates, 
            SqlConnection conn)
        {

            SqlTransaction transaction = conn.BeginTransaction("bson");
            var count = 0;
            var skip = 0;
            var action = collection.Find("{Entity_Type_Code : '2'}").ForEachAsync(b =>
            {
                string result = "";

                result = insert(b, conn, zipCodeTimeZones, zipCodeStates, transaction);

                if (result != "OK")
                    transaction.Rollback("bson");

                else
                    transaction.Commit();

                if (codes.ContainsKey(result))
                {
                    codes[result]++;
                }
                else
                {
                    codes.Add(result, 1);
                }
                count++;


                if (count == 10000)
                {
                    Console.Clear();
                    Console.CursorLeft = 0;
                    Console.CursorTop = 0;
                    foreach (var item in codes.Keys)
                    {
                        Console.WriteLine(item + "-" + codes[item]);

                    }
                    count = 0;
                }
            });

            action.Wait();
            Console.Read();
        }

        bool toggle = false;

        private string insert(BsonDocument b, SqlConnection conn, IMongoCollection<BsonDocument> zipCodeTimeZones
            , IMongoCollection<BsonDocument> zipCodeStates, SqlTransaction transaction)
        {
            try
            {

                var npi = get("_id", b);

                var practice_name = get("Org_Name_Legal_Business_Name",b);

                //provider table
                string query1 = string.Format(provider_query_template, npi);
                var cmd1 = new SqlCommand(query1, conn, transaction);
                var provider_key = (int)cmd1.ExecuteScalar();

                //practice table

                string query2 = string.Format(practice_query_template, provider_key, practice_name);
                var cmd2 = new SqlCommand(query2, conn, transaction);
                var practice_Identifier = ((string)cmd2.ExecuteScalar()).Trim(' ');

                var address = new Address(b, zipCodeStates);


                string addr_query = string.Format(address_query, "");
                var addr_Cmd = new SqlCommand(addr_query, conn, transaction);
                var address_key = (int)addr_Cmd.ExecuteScalar();

                var tz = getTimeZone(address.Zip, zipCodeTimeZones);
                var phone = getPhone(b);
                var fax = getFax(b, phone);

                //practice location
                string query3 = string.Format(practice_location_query, practice_name, practice_Identifier, address_key, tz, phone, fax);
                var cmd3 = new SqlCommand(query3, conn, transaction);
                var practice_location_key = (int)cmd3.ExecuteScalar();

                int mem_network = FindMemNetwork(address.Zip, conn, zipCodeStates);

                //practice member network
                string query4 = string.Format(practice_member_nw_query, practice_Identifier, mem_network); 
                var cmd4 = new SqlCommand(query4, conn, transaction);
                int result =cmd4.ExecuteNonQuery();

            }
            catch(Exception ex)
            {
                return "Exception";
            }
            return "OK";
        }

        private int FindMemNetwork(string zip, SqlConnection conn, IMongoCollection<BsonDocument> zipCodeStates)
        {
            var found = zipCodeStates.Find(string.Format("{\"Zip Code\" : '{0}'}", zip));
            var name = found.First().GetValue("State").AsString;
            SqlCommand foo = new SqlCommand(string.Format("select MemberNetworKey from network.MemberNetwork where Name like '{0}%'", name), conn);
            int result = (int)foo.ExecuteScalar();
            return result;

        }

        private string getPhone(BsonDocument b)
        {
            return getNum (b, new[]{ "BizAdd_Telephone_Number", "BizLocAdd_Telephone_Number",
                "BizAdd_Fax_Number", "BizLocAdd_Fax_Number" });
        }

        private string getFax(BsonDocument b, string num)
        {
            return getNum(b, new[] { "BizAdd_Fax_Number", "BizLocAdd_Fax_Number",
                "BizAdd_Telephone_Number", "BizLocAdd_Telephone_Number" },num);
        }

        private string getNum(BsonDocument b, string[] fields, string num ="")
        {
            long value; 
            foreach (var field in fields)
            {
                if (long.TryParse(get(field, b), out value))
                {
                    if (value.ToString().Length == 10 && value.ToString() != num)
                        return value.ToString();
                }
            }
            if (num != string.Empty)
                return num;
            else
                return new Random(1000000000).Next().ToString();

        }

        private string getTimeZone(string zip , IMongoCollection<BsonDocument> zipCodeTimeZones)
        {
            var query = string.Format("{{zip : '{0}'}}", zip);
            var result = zipCodeTimeZones.Find(query);

            var timezone = result.FirstOrDefault();

            BsonElement element;
            if(timezone.TryGetElement("timezone", out element))
            {
                return element.Value.AsString;

            }

            return null;

        }



        const string provider_query_template = @"insert into provider.provider 
                                               (ProviderType, NationalProviderIdentifier, IsAcceptingReferral, 
                                                IsSearchable, IsActive, Description, CreatedBy) 
                                                OUTPUT inserted.ProviderKey 
                                                values (1, '{0}', 1,1,1, 'This is an awesome practice', 1)";

        const string practice_query_template = @"insert into provider.Practice
                                               (Provider,Name, PracticeIdentifier,PracticeType) 
                                               OUTPUT inserted.PracticeIdentifier
                                               values ({0}, '{1}', STR(CHECKSUM(NewId()) % 1000000), 'Practice')";

        const string practice_location_query = @"insert into provider.PracticeLocation
                                               (Name, Practice, Address, TimeZone, PrimaryPhone,PrimaryFax, 
                                               IsPrimaryLocation, IsActive, CreatedBy, IsFaxOnly, IsAmbulatory)
                                               OUTPUT INSERTED.PracticeLocationkey
                                               values ('{0}', '{1}', {2}, '{3}', '{4}', '{5}', 1,1,1, CAST(ROUND(RAND(),0) AS BIT), 1)";

        const string practice_member_nw_query = @"insert into provider.PracticeMemberNetwork
                                                (Prakctice, MemberNetwork, PracticeMemberNetworkState, IsActive, CreatedBy)
                                                values ({0}, {1}, 3, 1,1)";

        const string address_query = @"insert into provider.Address (AddressLine1, AddressLine2, City, State, ZipCode, CreatedBy) 
                                        output inserted.AddressKey values ('{0}', '{1}', '{2}', '{3}', '{4}', 1)";


        private static string get(string name, BsonDocument b)
        {

            string item = "";
            if (b.Contains(name))
                item = b.GetValue(name).AsString.Trim();


            return item;
        }
    }
}
