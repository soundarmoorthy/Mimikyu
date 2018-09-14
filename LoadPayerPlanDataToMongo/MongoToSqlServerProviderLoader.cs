using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace LoadPayerPlanDataToMongo

{
    public class MongoToSqlServerPracticeLoader
    {
        Dictionary<string, int> codes = new Dictionary<string, int>();

        SqlConnection conn;
        IMongoCollection<BsonDocument> inserted;
        SqlTransaction transaction;
        private readonly string practicesQuery = "{enumeration_type : 'NPI-2'}";

        int unique;
        public async void Load()
        {
            IMongoCollection<BsonDocument> collection = Initialize();
            using (conn = new SqlConnection("Data Source=.;Initial Catalog=ReferralNetwork;Integrated Security=True"))
            {
                conn.Open();
                Run(collection);
            }
        }

        private IMongoCollection<BsonDocument> Initialize()
        {
            var url = "mongodb://localhost:27017";
            var client = new MongoClient(url);
            var database = client.GetDatabase("US");
            var collection = database.GetCollection<BsonDocument>("provider");
            inserted = database.GetCollection<BsonDocument>("inserted");
            unique = (int)inserted.Count("{}") + 1;
            return collection;
        }

        private void Run(IMongoCollection<BsonDocument> collection)
        {
            var practices = collection.Find(practicesQuery);

            var task = practices.ForEachAsync(b =>
            {
                var p = new Provider(b).Location();
                var result  = Process(p);
                Trace(result);
            });

            task.Wait();


        }

        int count;
        private string Process(Provider p)
        {

            //If already present return
            var completed = inserted.Find($"{{_id: '{p.Id}' }}");
            if (completed.Any())
                return "Skipped";

            //Now insert the record
            transaction = conn.BeginTransaction(p.Id);
            string result = Insert(p);

            if (result != "OK")
            {
                transaction.Rollback(p.Id);
            }
            else
            {
                transaction.Commit();
                inserted.InsertOne(new BsonDocument("_id", p.Id));
                unique++;
            }

            return result;

        }

        private string Trace(string result)
        {
            if (codes.ContainsKey(result))
            {
                codes[result]++;
            }
            else
            {
                codes.Add(result, 1);
            }
            count++;


            if (count == 2000)
            {
                Console.Clear();
                Console.CursorLeft = 0;
                Console.CursorTop = 0;
                StringBuilder b = new StringBuilder();

                foreach (var item in codes.Keys)
                {
                    b.AppendLine($"{item} - {codes[item]}");
                }

                Console.WriteLine(b);
                File.WriteAllText("log.log", b.ToString());
                count = 0;
            }

            return result;
        }

        private string Insert(Provider p)
        {
            try
            {
                int providerKey = 0;
                if (!Exists(p.NPI, out providerKey))
                {
                    providerKey = SetupPractice(p);
                }
                else
                    return "Practice Exists";

                return SetupLocation(p, providerKey);

            }
            catch (SqlException se)
            {
                return se.Message;
            }
            catch(Exception e)
            {
                return e.ToString();
            }
        }

        private string SetupLocation(Provider p, int providerKey)
        {
            var orgName = p.OrgName;
            if (LocationExists(orgName))
            {
                orgName = $"{orgName}-{p.City}-{p.ZipCode}";
            }

            if (LocationExists(orgName))
                return "Location Damn Exists";

            //Address
            var query = GetAddressQuery(p);
            var cmd = new SqlCommand(query, conn, transaction);
            var addrKey = (int)cmd.ExecuteScalar();

            //practice location
            var tz = GetTimeZone(p.StateCode);
            query = string.Format(practice_location_query, p.OrgName, providerKey, addrKey, tz, p.Phone, p.Fax, unique);
            cmd = new SqlCommand(query, conn, transaction);
            var praLocKey = (int)cmd.ExecuteScalar();

            return "OK";
        }

        private int SetupPractice(Provider p)
        {
            var npi = p.NPI;
            var pName = p.OrgName;
            //provider table
            var query = string.Format(provider_query_template, npi, p.Description);
            var cmd = new SqlCommand(query, conn, transaction);
            var proKey = (int)cmd.ExecuteScalar();

            //practice table
            query = string.Format(practice_query_template, proKey, pName, unique);
            cmd = new SqlCommand(query, conn, transaction);
            cmd.ExecuteNonQuery();

            int mem_network = FindMemNetwork(p.StateCode, p.CountryCode);

            //practice member network
            query = string.Format(practice_member_nw_query, proKey, mem_network);
            cmd = new SqlCommand(query, conn, transaction);
            cmd.ExecuteNonQuery();

            return proKey;
        }

        private bool LocationExists(string orgName)
        {
            var query = $"select PracticeLocationKey from provider.practiceLocation where Name='{orgName}'";
            var comnd = new SqlCommand(query, conn, transaction);
            var reslt = comnd.ExecuteScalar();
            return reslt != null && ((int)reslt) > 0;
        }


        private bool Exists(string npi, out int providerKey)
        {
            var query = $"select ProviderKey from Provider.Provider where NationalProviderIdentifier='{npi}'";
            var cmd = new SqlCommand(query, conn, transaction);
            var result = cmd.ExecuteScalar()?.ToString();
            return int.TryParse(result, out providerKey) && providerKey > 0;
        }

        private int GetTimeZone(string stateCode)
        {
            return USStateToTimeZone.TimeZoneKey(stateCode);

        }

        private string GetAddressQuery(Provider p)
        {
            //insert into provider.Address (AddressLine1, AddressLine2, City, State, ZipCode, CreatedBy) 

            string query = string.Empty;
            if (p.CountryCode != "US" || USPacificIslands(p.StateCode))
                query = string.Format(address_query_template_non_us, p.AddrLine1, p.AddrLine2, p.City, p.ZipCode);
            else
                query = string.Format(address_query_template, p.AddrLine1, p.AddrLine2, p.City, p.StateCode, p.ZipCode);

            return query;

        }

        private int FindMemNetwork(string stateCode, string countryCode)
        {
            if (countryCode != "US" || USPacificIslands(stateCode))
                return 106; //Return generic network.

            if (states.ContainsKey(stateCode))
                return states[stateCode];

            string stateName = string.Empty;
            USCodeToStates.Map.TryGetValue(stateCode, out stateName);
            SqlCommand foo = new SqlCommand(string.Format(find_network_query, stateName), conn, transaction);
            var res = foo.ExecuteScalar();
            var result = (int)res;
            states.Add(stateCode, result);
            return result;

        }

        List<string> pacific = new List<string>{ "AP", "GU", "VI", "PW","FM","AA", "AS", "MP" };
        StringCaseInsensitiveComparer comparer = new StringCaseInsensitiveComparer();
        private bool USPacificIslands(string stateCode)
        {
            return pacific.Contains(stateCode, comparer);
        }

        Dictionary<string, int> states = new Dictionary<string, int>();


        const string provider_query_template = @"insert into provider.provider 
                                               (ProviderType, NationalProviderIdentifier, IsAcceptingReferral, 
                                                IsSearchable, IsActive, Description, CreatedBy) 
                                                OUTPUT inserted.ProviderKey 
                                                values (1, '{0}', 1,1,1, '{1}', 1)";

        const string practice_query_template = @"insert into provider.Practice
                                               (Provider,Name, PracticeIdentifier,PracticeType) 
                                               OUTPUT inserted.PracticeIdentifier
                                               values ({0}, '{1}', {2}, 'Practice')";

        const string practice_location_query = @"insert into provider.PracticeLocation
                                               (Name, Practice, Address,TimeZone, PrimaryPhone,PrimaryFax, 
                                               IsPrimaryLocation, IsActive, CreatedBy, IsFaxOnly, IsAmbulatory, PracticeLocationIdentifier)
                                               OUTPUT INSERTED.PracticeLocationkey
                                               values ('{0}',{1},{2},{3}, '{4}', '{5}', 1,1,1,CAST(rand()*10 as INT) % 2, CAST(rand()*10 as INT) % 2, {6})";

        const string practice_member_nw_query = @"insert into provider.PracticeMemberNetwork
                                                (Practice, MemberNetwork, PracticeMemberNetworkState, IsActive, CreatedBy)
                                                values ({0}, {1}, 3, 1,1)";

        const string address_query_template = @"insert into provider.Address (AddressLine1, AddressLine2, City, State, ZipCode, CreatedBy) 
                                              output inserted.AddressKey values ('{0}', '{1}', '{2}', '{3}', '{4}', 1)";

        const string address_query_template_non_us = @"insert into provider.Address (AddressLine1, AddressLine2, City, ZipCode, CreatedBy) 
                                              output inserted.AddressKey values ('{0}', '{1}', '{2}', '{3}', 1)";


        const string find_network_query = "select MemberNetworkKey from network.MemberNetwork where LOWER(SUBSTRING(NAME, 0,CHARINDEX(' Network',Name,0))) = LOWER('{0}')";

    }
}
