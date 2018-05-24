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

    class MongoToSqlServerPatientLoader
    {
        const string pQueryTemplate = "insert into provider.Patient (FirstName, MiddleName, LastName, DateOfBirth, Address, PatientPhone, PatientPhoneExtension, Gender, CreatedBy, SecondaryPhone) values('{0}','{1}', '{2}', '{3}', {4}, '{5}', '{6}', '{7}', 1, '{8}')";

        const string aQueryTemplate = "insert into provider.Address (AddressLine1, AddressLine2, City, State, ZipCode, CreatedBy) output inserted.AddressKey values('{0}', '{1}', '{2}', '{3}', '{4}', 1);";

        private SqlConnection conn;

        Dictionary<string, int> codes = new Dictionary<string, int>();

        public MongoToSqlServerPatientLoader()
        {
            conn = new SqlConnection("Data Source=.;Initial Catalog=CMR;Integrated Security=True");

        }


        public void Run()
        {
            var url = "mongodb://localhost:27017";
            var client = new MongoClient(url);
            var database = client.GetDatabase("US");
            var collection = database.GetCollection<BsonDocument>("People");

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


               if (count == 5000)
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

        private string insert(BsonDocument b)
        {
            //FirstName, MiddleName, LastName, DateOfBirth, Address, PatientPhone, PatientPhoneExtension, Gender, CreatedBy, SecondaryPhone
            //(AddressLine1, AddressLine2, City, State, ZipCode, CreatedBy
            var a1 = get("AddressLine1",b);
            var a2 = get("AddressLine2",b);
            var a3 = get("City",b);

            var a4 = get("State",b);

            if (a4 == "PR" || a4 == "VI")
                return "PR not valid state";

            var a5 = get("Zipcode",b);
            var addr_query = string.Format(aQueryTemplate, a1, a2, a3, a4, a5);
            int addr_res = -1;


            SqlCommand addr_cmd = new SqlCommand(addr_query, conn);
            try
            {
                addr_res = (int)addr_cmd.ExecuteScalar();
            }
            catch (SqlException e)
            {
                return "Exception";
            }

            var p1 = get("FirstName", b);
            var p2 = get("MiddleName", b);
            var p3 = get("LastName", b);
            var p4 = get("DateOfBirth", b);
            var p5 = get("Phone", b);
            var p6 = get("SecondaryPhone", b);
            var p7 = get("Extn", b);
            var p8 = get("Gender\r", b);


            var patient_query = string.Format(pQueryTemplate, p1, p2, p3, p4, addr_res, p5, p7, p8, p6);
            SqlCommand patient_cmd = new SqlCommand(patient_query, conn);

            try
            {
                patient_cmd.ExecuteNonQuery();
            }
            catch (SqlException e)
            {
                return "Exception";
            }

            return "OK";
        }


        private string get(string name, BsonDocument b)
        {

            string item = string.Empty;
            if (b.Contains(name))
                item = b.GetValue(name).AsString.Trim('\r').Replace("\'", "");


            return item;
        }

       
    }
}
