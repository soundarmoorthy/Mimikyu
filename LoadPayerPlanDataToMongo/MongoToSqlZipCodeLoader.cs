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
    class MongoToSqlZipCodeLoader
    {
        private SqlConnection conn;
        public MongoToSqlZipCodeLoader()
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

        const string queryTemplate = "insert into geo.ZipCodePayerPlan (ZipCode,PayerPlan) values ({0},{1})";


        Dictionary<string, int> codes = new Dictionary<string, int>();


        private string insert(BsonDocument b)
        {
            string orgName = string.Empty;
            if (b.Contains("legal_entity_name"))
                orgName = b.GetValue("legal_entity_name").AsString;
            else
                return "No Payer";

            string planName = string.Empty;

            if (b.Contains("plan_name"))
                planName = b.GetValue("plan_name")?.AsString;
            else
                return "No Plan Name";

            if (string.IsNullOrEmpty(planName))
                return "No Plan Name";

            if (planName.IndexOf('\'') != -1)
                planName = planName.Replace("'", "''");

            var zipCode = "";
            if (b.Contains("zip_code"))
                zipCode = b.GetValue("zip_code")?.AsString;
            else
                return "No Zip Code";

            var countFipsCode = b.GetValue("CountyFIPSCode")?.AsString ?? string.Empty;

            planName = string.Join("-", countFipsCode, planName);
            SqlCommand cmd = new SqlCommand(string.Format("select PayerPlanKey from provider.payerplan where Name='{0}'", planName), conn);
            var result = cmd.ExecuteScalar();
            if (result != null && (int)result > 1)
            {

                var query = string.Format(queryTemplate, zipCode, (int)result);
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
            else
                return "No Plan Found";
           
        }
    }
}