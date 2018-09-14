using LoadPayerPlanDataToMongo.Patient;
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
        const string addrQuery = "insert into provider.Address (AddressLine1, AddressLine2, City, State, ZipCode, CreatedBy) output inserted.AddressKey values('{0}', '{1}', '{2}', '{3}', '{4}', 1);";
        const string patientQuery = "insert into provider.Patient (FirstName, LastName, DateOfBirth, Address, PatientPhone, Gender, CreatedBy) values ('{0}','{1}', '{2}', {3}, '{4}', '{5}', 1 )";

        private SqlConnection conn;
        Dictionary<string, int> codes = new Dictionary<string, int>();

        public MongoToSqlServerPatientLoader()
        {
            conn = new SqlConnection
                ("Data Source=.;Initial Catalog=MemberNetwork;Integrated Security=True");

        }


        public void Run()
        {
            var url = "mongodb://localhost:27017";
            var client = new MongoClient(url);
            var database = client.GetDatabase("US");
            var collection = database.GetCollection<BsonDocument>("NewPeople");
            conn.Open();


            var count = 0;
            var action = collection.Find("{}").ForEachAsync(b =>
           {

               var result = insert(b);

               if (codes.ContainsKey(result))
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
            //, PatientPhoneExtension, Gender, CreatedBy, SecondaryPhone
            //(AddressLine1, AddressLine2, City, State, ZipCode, CreatedBy

            SqlTransaction transaction = conn.BeginTransaction();
            try
            {

                var p = new PatientParseStrategy2().Parse(b);

                var addr_query = string.Format
                    (addrQuery, p.AddressLine1, p.AddressLine2, p.City, p.State, p.Zipcode);

                int addrKey = -1;

                SqlCommand addr_cmd = new SqlCommand(addr_query, conn, transaction);
                addrKey = (int)addr_cmd.ExecuteScalar();

                var patient_query = string.Format(patientQuery,
                    p.Firstname, p.Lastname, p.DateOfBirth, addrKey, p.Phone, p.Gender);
                SqlCommand patient_cmd = new SqlCommand(patient_query, conn, transaction);

                patient_cmd.ExecuteNonQuery();
            }
            catch (SqlException e)
            {
                transaction.Rollback();
                return "Exception";

            }
            transaction.Commit();

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
