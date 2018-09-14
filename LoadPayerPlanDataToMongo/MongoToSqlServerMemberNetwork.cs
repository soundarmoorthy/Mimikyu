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
    class MongoToSqlServerMemberNetwork
    {

        const string queryTemplate =

        @"INSERT INTO [network].[MemberNetwork]
           ([Name]
           ,[IsActive]
           ,[CreatedBy]
           ,[CreatedOn]
           ,[OutOfNetworkDialogTitle]
           ,[OutOfNetworkDialogInstructions]
           ,[OutOfNetworkToolTip]
           ,[ZipRadius]
           ,[CanSearchOutOfNetwork]
           ,[AutoCheckElectronicallySigned]
           ,[CanRedirectReferrals]
           ,[CanBookOnline]
           ,[CanSendWithoutFaxing]
           ,[CanAccessUnassignedReferrals]
           ,[CanUpdateReferralOrders]
           ,[CanSendReferralByDirect]
           ,[AlwaysSendDirectMessageToPractice])
     VALUES(
           '{0}',
            1,
            1,
            CURRENT_TIMESTAMP,
           'Out of Network Dialog',
           'Contact the project manager for more details',
           'Sure to loose some money',
            40,
            ABS(CHECKSUM(NewId())) % 2,
            ABS(CHECKSUM(NewId())) % 2,
            ABS(CHECKSUM(NewId())) % 2,
			1,
			1,
			1,
			1,
			1,

            ABS(CHECKSUM(NewId())) % 2);";


        private SqlConnection conn;

        Dictionary<string, int> codes = new Dictionary<string, int>();

        public MongoToSqlServerMemberNetwork()
        {
            conn = new SqlConnection("Data Source=.;Initial Catalog=ReferralNetwork;Integrated Security=True");

        }


        public void Run()
        {
            var url = "mongodb://localhost:27017";
            var client = new MongoClient(url);
            var database = client.GetDatabase("US");
            var collection = database.GetCollection<BsonDocument>("States");

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


               foreach (var item in codes.Keys)
               {
                   Console.WriteLine(item + "-" + codes[item]);

               }
               count = 0;

           });

            action.Wait();
            conn.Close();
        }

        private string insert(BsonDocument b)
        {
            var a1 = get("State",b);

            if (a1.ToLower() == "texas")
                return "Exists";

            a1 = a1 + " Network";

            var addr_query = string.Format(queryTemplate, a1);


            var query = string.Format(queryTemplate, a1);
            SqlCommand cmd = new SqlCommand(query, conn);

            try
            {
                cmd.ExecuteNonQuery();
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
