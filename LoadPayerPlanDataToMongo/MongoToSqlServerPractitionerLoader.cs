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
    public class MongoToSqlServerPractitionerLoader
    {
        Dictionary<string, int> codes = new Dictionary<string, int>();

        SqlConnection conn;
        IMongoCollection<BsonDocument> inserted;
        SqlTransaction transaction;
        private readonly string practitionerQuery = "{enumeration_type : 'NPI-1'}";

        int unique;
        public async void Load()
        {
            IMongoCollection<BsonDocument> collection = Initialize();
            using (conn = new SqlConnection("Data Source=.;Initial Catalog=CMR;Integrated Security=True"))
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
            inserted = database.GetCollection<BsonDocument>("inserted_practitioner");
            unique = (int)inserted.Count("{}") + 1;
            return collection;
        }

        private void Run(IMongoCollection<BsonDocument> collection)
        {
            var practices = collection.Find(practitionerQuery);

            var task = practices.ForEachAsync(b =>
            {
                var p = new Provider(b).Location();
                var result = Process(p);
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


            if (count == 1000)
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
                if (Exists(p.NPI))
                    return "SKIPPED";

                SetupPractitioner(p);
                return "OK";
            }
            catch (SqlException se)
            {
                return se.Message;
            }
            catch (Exception e)
            {
                return e.Message;
            }

        }

        private bool Exists(string npi)
        {
            int providerKey = 0;
            var query = $"select ProviderKey from Provider.Provider where NationalProviderIdentifier='{npi}'";
            var cmd = new SqlCommand(query, conn, transaction);
            var result = cmd.ExecuteScalar()?.ToString();
            return int.TryParse(result, out providerKey) && providerKey > 0;
        }


        private int SetupPractitioner(Provider p)
        {
            //provider
            var query = string.Format(provider_query_template, p.NPI, p.Description);
            var cmd = new SqlCommand(query, conn, transaction);
            var proKey = (int)cmd.ExecuteScalar();

            //practitioner table
            query = $"INSERT INTO [provider].[Practitioner] VALUES({proKey}, '{p.Firstname}', '{p.Middlename}', '{p.Lastname}', '{p.Gender}', '{p.Credential}', '{p.Suffix}', '{unique}')";
            cmd = new SqlCommand(query, conn, transaction);
            cmd.ExecuteNonQuery();

            //practice location
            var pLocKey = PracticeLocation(p.ZipCode, p.CountryCode);
            query = $"INSERT INTO [provider].[PractitionerPracticeLocation] (Practitioner, PracticeLocation, CreatedBy) VALUES({proKey}, '{pLocKey}', 1)";
            cmd = new SqlCommand(query, conn, transaction);
            cmd.ExecuteNonQuery();

            //subspecialty
            var sp = Subspecialty(pLocKey);
            var pri = 1;
            foreach (var s in sp)
            {
                query = $"Insert into provider.PractitionerSubSpecialty (Practitioner, Subspecialty, IsPrimary, IsBoardCertified, CreatedBy) VALUES ({proKey}, {s}, {pri},1,1)";
                cmd = new SqlCommand(query, conn, transaction);
                cmd.ExecuteNonQuery();
                pri = 0;
            }

            var payerPlans = PractitionerPayerPlans(p.ZipCode);
            foreach (var pp in payerPlans)
            {
                query = $"Insert into provider.ProviderPayerPlan (Provider,PayerPlan, CreatedBy) VALUES ({proKey}, {pp}, 1)";
                cmd = new SqlCommand(query, conn, transaction);
                cmd.ExecuteNonQuery();
            }


            string stateCode = p.StateCode;
            if (stateCode.Length > 2 || !USCodeToStates.Map.ContainsKey(stateCode))
                stateCode = "TX";
            query = $"Insert into provider.PractitionerLicense (Practitioner, LicenseNumber, State, CreatedBy) VALUES ({proKey}, '{p.License}', '{stateCode}', 1)";
            cmd = new SqlCommand(query, conn, transaction);
            cmd.ExecuteNonQuery();

            return proKey;
        }

        private IEnumerable<int> PractitionerPayerPlans(string strZip)
        {
            int zip = 0;
            if (!int.TryParse(strZip, out zip))
                throw new InvalidCastException();

            if (plansByZip.ContainsKey(zip))
                return plansByZip[zip].RandomSubset(33);

            var query = $"Select PayerPlan from geo.ZipCodePayerPlan where ZipCode='{zip}'";
            SqlCommand cmd = new SqlCommand(query, conn, transaction);
            using (var rs = cmd.ExecuteReader())
            {
                List<int> plans = new List<int>();
                while (rs.Read())
                {
                    var plan = (int)rs.GetSqlInt32(0);
                    plans.Add(plan);
                }
                rs.Close();

                ContentIterator c = new ContentIterator(plans);
                plansByZip.Add(zip, c);
                return c.RandomSubset(33);
            }
        }

        private IEnumerable<int> Subspecialty(int proKey)
        {
            if (subsByPractice.ContainsKey(proKey))
                return subsByPractice[proKey].RandomSubset();

            var query = $"Select SubSpecialty from provider.PracticeSubSpecialty s join provider.PracticeLocation p on s.Practice = p.Practice  where p.PracticeLocationKey={proKey}";
            SqlCommand cmd = new SqlCommand(query, conn, transaction);
            using (var rs = cmd.ExecuteReader())
            {
                List<int> subs = new List<int>();
                while (rs.Read())
                {
                    var spe = (int)rs.GetSqlInt32(0);
                    subs.Add(spe);
                }
                rs.Close();
                ContentIterator c = new ContentIterator(subs);
                subsByPractice.Add(proKey, c);

                return c.RandomSubset();
            }

        }

        private int PracticeLocation(string zip, string countryCode)
        {
            if (countryCode != "US")
                return PracticeLocationOpenMaket.Next();
            int zipCode = 0;
            if (!int.TryParse(zip, out zipCode))
                throw new InvalidCastException();

            if (practiceByZip.ContainsKey(zipCode))
                return practiceByZip[zipCode].Next();

            var query = $"select PracticeLocationKey from provider.practiceLocation pl join provider.Address a on pl.address = a.Addresskey where a.ZipCode='{zipCode}'";
            var comnd = new SqlCommand(query, conn, transaction);
            using (var rs = comnd.ExecuteReader())
            {

                if (!rs.HasRows)
                    return PracticeLocationOpenMaket.Next();

                List<int> keys = new List<int>();
                while (rs.Read())
                {
                    var key = (int)rs.GetSqlInt32(0);
                    keys.Add(key);
                }

                rs.Close();
                ContentIterator loc = new ContentIterator(keys);
                practiceByZip.Add(zipCode, loc);
                return loc.Next();
            }
        }

        Dictionary<int, ContentIterator> practiceByZip = new Dictionary<int, ContentIterator>();
        Dictionary<int, ContentIterator> plansByZip = new Dictionary<int, ContentIterator>();
        Dictionary<int, ContentIterator> subsByPractice = new Dictionary<int, ContentIterator>();


        const string provider_query_template = @"insert into provider.provider 
                                               (ProviderType, NationalProviderIdentifier, IsAcceptingReferral, 
                                                IsSearchable, IsActive, Description, CreatedBy) 
                                                OUTPUT inserted.ProviderKey 
                                                values (2, '{0}', 1,1,1, '{1}', 1)";
    }
}
