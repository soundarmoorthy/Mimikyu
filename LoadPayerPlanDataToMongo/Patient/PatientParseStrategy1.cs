using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using Newtonsoft.Json.Linq;

namespace LoadPayerPlanDataToMongo.Patient
{
    class PatientParseStrategy1 : IParseStrategy
    {
        JObject json;
        public Patient Parse(BsonDocument b)
        {
            Patient p = new Patient();
            p.Id = b.GetValue("_id").ToString();
            var json = JObject.Parse(b.ToString());

            p.AddressLine1 = json["AddressLine1"].ToString();
            p.AddressLine2 = json["AddressLine2"].ToString();
            p.City = json["City"].ToString();
            p.State = json["State"].ToString();
            p.Zipcode = json["Zipcode"].ToString();

            //if (a4 == "PR" || a4 == "VI")
            //    return "PR not valid state";

            p.Firstname = json["FirstName"].ToString();
            p.Middlename = json["MiddleName"].ToString();
            p.Lastname = json["LastName"].ToString();
            p.DateOfBirth = json["DateOfBirth"].ToString();
            p.Phone = json["Phone"].ToString();
            p.SecondaryPhone = json["SecondaryPhone"].ToString();
            p.Extn = json["Extn"].ToString();
            p.Gender = json["Gender\r"].ToString();


            return p;
        }
    }
}
