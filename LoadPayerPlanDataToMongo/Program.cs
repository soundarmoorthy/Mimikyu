﻿using System;
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
            //MongoPayerLoader loader = new MongoPayerLoader();
            //loader.Load();

            MongoToSqlServerPayerLoader p = new MongoToSqlServerPayerLoader();
            p.QueryLoad();

        }
    }
}