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
    class MongoToSqlServerPayerLoader
    {
        private SqlConnection conn;
        public MongoToSqlServerPayerLoader()
        {
            
            conn = new SqlConnection("Data Source=.;Initial Catalog=CMR;Integrated Security=True");

        }


        public void QueryLoad()
        {

            var url = "mongodb://localhost:27017";
            var client = new MongoClient(url);
            var database = client.GetDatabase("US");
            var collection = database.GetCollection<BsonDocument>("PayerPlan");

            conn.Open();


            var count = 0;
            var action = collection.Find("{}").ForEachAsync(b =>
           {

               var result = insert(b);

               if(codes.ContainsKey(result))
               {
                   codes[result]++;
               }
               else
               {
                   codes.Add(result, 1);
               }
               count++;


               if (count == 25000)
               {
                   foreach (var item in codes.Keys)
                   {
                       Console.WriteLine(item + "-" + codes[item]);

                   }
                   count = 0;

               }
           });

            action.Wait();
            conn.Close();


        }

        const string queryTemplate = "insert into provider.PayerPlan (Name, Payer, IsActive,CreatedBy) values ('{0}',{1},1,1)";


        Dictionary<string, int> codes = new Dictionary<string, int>();


        private string insert(BsonDocument b)
        {
            string orgName = string.Empty;
            if (b.Contains("legal_entity_name"))
                orgName = b.GetValue("legal_entity_name").AsString;
            else
                return "No Payer";

            string planName = string.Empty;
            string city = string.Empty;
            string state_code = string.Empty;

            if (b.Contains("plan_name"))
                planName = b.GetValue("plan_name")?.AsString;
            else
                return "No Plan Name";

            int payerKey = -1;

            if (!Payer.Lookup.TryGetValue(orgName, out payerKey))
                return "No payer in Lookup";

            if (string.IsNullOrEmpty(planName))
                return  "No Plan Name";

            if (payerKey == -1)
                return "No payer in Lookup";

            if (planName.IndexOf('\'') != -1)
                planName = planName.Replace("'", "''");

            SqlCommand cmd = new SqlCommand(string.Format("select PayerPlanKey from provider.payerplan where Name='{0}'", planName), conn);
            var result = cmd.ExecuteScalar();

            if (result != null && (int)result != 0)
            {
                if (b.Contains("CountyFIPSCode"))
                {
                    city = b.GetValue("CountyFIPSCode")?.AsString;

                    planName = string.Join("-", city, planName);
                    SqlCommand cmd1 = new SqlCommand(string.Format("select PayerPlanKey from provider.payerplan where Name='{0}'", planName), conn);
                    var result1 = cmd1.ExecuteScalar();
                    if (result1 != null && (int)result1 != 0)
                    {
                        return "Plan name + city duplicate";

                    }
                }
                else
                    return "No city entry";
            }

            var query = string.Format(queryTemplate, planName, payerKey);
            SqlCommand command = new SqlCommand(query, conn);
            try
            {
                command.ExecuteNonQuery();
                return "OK";
            }
            catch (System.Data.SqlClient.SqlException ex)
            {
                return "Exception";
            }
        }
    }
}