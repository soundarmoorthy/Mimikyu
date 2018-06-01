
using System.Collections.Generic;

namespace LoadPayerPlanDataToMongo
{
    class USStateToTimeZone
    {

        public static int TimeZoneKey(string state)
        {
            int tz;
            if (Map.TryGetValue(state, out tz))
            {
                int cmrKey = 0;
                if (cmrMap.TryGetValue(tz, out cmrKey))
                    return cmrKey;
            }
            return 0;
        }

        private static Dictionary<int, int> cmrMap = new Dictionary<int, int>()
        {
{-10,0},
{-9, 1},
{-8, 2},
{-7, 3},
{-6, 4},
{-5, 5},
{-4, 6}
        };

        private static Dictionary<string, int> Map = new Dictionary<string, int>()
        {
{"NH"  ,   -5},
{"NY"  ,   -5},
{"PR"  ,   -4},
{"VI"  ,   -4},
{"MA"  ,   -5},
{"RI"  ,   -5},
{"ME"  ,   -5},
{"VT"  ,   -5},
{"CT"  ,   -5},
{"NJ"  ,   -5},
{"PA"  ,   -5},
{"DE"  ,   -5},
{"DC"  ,   -5},
{"VA"  ,   -5},
{"MD"  ,   -5},
{"WV"  ,   -5},
{"NC"  ,   -5},
{"SC"  ,   -5},
{"GA"  ,   -5},
{"TN"  ,   -5},
{"FL"  ,   -5},
{"AL"  ,   -6},
{"AR"  ,   -6},
{"MS"  ,   -6},
{"KY"  ,   -5},
{"OH"  ,   -5},
{"IN"  ,   -5},
{"MI"  ,   -6},
{"IA"  ,   -6},
{"IL"  ,   -6},
{"WI"  ,   -6},
{"MN"  ,   -6},
{"SD"  ,   -6},
{"ND"  ,   -7},
{"MT"  ,   -7},
{"MO"  ,   -6},
{"KS"  ,   -6},
{"NE"  ,   -6},
{"LA"  ,   -6},
{"OK"  ,   -6},
{"TX"  ,   -6},
{"NM"  ,   -7},
{"CO"  ,   -7},
{"WY"  ,   -7},
{"ID"  ,   -7},
{"UT"  ,   -7},
{"AZ"  ,   -7},
{"NV"  ,   -8},
{"CA"  ,   -8},
{"HI"  ,   -10},
{"AS"  ,   -10},
{"OR"  ,   -8},
{"WA"  ,   -8},
{"AK"  ,   -9},
};
    }
}
