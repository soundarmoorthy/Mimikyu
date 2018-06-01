using MongoDB.Bson;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LoadPayerPlanDataToMongo.Patient
{
    class Patient
    {
        public string AddressLine1 { get; internal set; }
        public string AddressLine2 { get; internal set; }
        public string City { get; internal set; }
        public string DateOfBirth { get; internal set; }
        public string Extn { get; internal set; }
        public string Firstname { get; internal set; }
        public string Gender { get; internal set; }
        public string Id { get; internal set; }
        public string Lastname { get; internal set; }
        public string Middlename { get; internal set; }
        public string Phone { get; internal set; }
        public string SecondaryPhone { get; internal set; }
        public string State { get; internal set; }
        public string Zipcode { get; internal set; }
    }
}
