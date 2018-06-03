using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

using System.Threading;
using MongoDB.Driver;
using MongoDB.Bson;
using System.Collections.Concurrent;

namespace LoadPayerPlanDataToMongo
{

    class Program
    {
        static void Main(string[] args)
        {
            //int a = 0;

            //int count = 10;
            //int b = 0;

            //while (true)
            //{
            //    a++;
            //    Console.WriteLine(a % count);

            //    if (a == count)
            //    {
            //        a = 0;
            //        b++;
            //    }

            //    if (b == 2)
            //        break;

            //}

            //MongoPayerLoader loader = new MongoPayerLoader();
            //loader.Load();

            //MongoToSqlZipCodeLoader p = new MongoToSqlZipCodeLoader();
            //p.QueryLoad();

            //MongoToSqlServerPatientLoader loader = new MongoToSqlServerPatientLoader();
            //loader.Run();

            //MongoToSqlServerMemberNetwork loader = new MongoToSqlServerMemberNetwork();
            //loader.Run();

            //MongoToSqlServerPracticeLoader loader = new MongoToSqlServerPracticeLoader();
            //loader.Load();

            //NPPESLoader loader = new NPPESLoader();
            //loader.Run();

            //ProviderLanguageRandomizerLoader loader = new ProviderLanguageRandomizerLoader();
            //loader.Load();

            //ProviderSubSpecialityLoader loader = new ProviderSubSpecialityLoader();
            //loader.Load();

            MongoToSqlServerPractitionerLoader loader = new MongoToSqlServerPractitionerLoader();
            loader.Load();
        }
    }
}
