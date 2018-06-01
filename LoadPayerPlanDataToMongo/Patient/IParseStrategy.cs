using MongoDB.Bson;

namespace LoadPayerPlanDataToMongo.Patient
{
    internal interface IParseStrategy
    {
        Patient Parse(BsonDocument b);
    }
}