﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LoadPayerPlanDataToMongo
{
    class PracticeLocationOpenMaket
    {

        static ContentIterator iterator;

        public static int Next()
        {
            if (iterator == null)
                iterator = new ContentIterator(list);
            return iterator.Next();
        }


        private static readonly List<int> list = new List<int>() { 1017,
1052,
1053,
1054,
1056,
1057,
1058,
1059,
1060,
1061,
1062,
1069,
1983,
3813,
8764,
9349,
9841,
9931,
9932,
9933,
9934,
9935,
9936,
9937,
9941,
10249,
10250,
10251,
10252,
10253,
12891,
12892,
12939,
14015,
14016,
14017,
14018,
14019,
14020,
14021,
14023,
14024,
14025,
14026,
14027,
14028,
14217,
14218,
14219,
15556,
16112,
16113,
16114,
16115,
16116,
16117,
16118,
16119,
16120,
16121,
16122,
16123,
16124,
16125,
16126,
16127,
16128,
16129,
16130,
16131,
16132,
16133,
16134,
16135,
16137,
16138,
16139,
16140,
16141,
16142,
16143,
16144,
16145,
16146,
16147,
16148,
16149,
16150,
16380,
16381,
16382,
16383,
16384,
16385,
16386,
16387,
16388,
16389,
16390,
16391,
16392,
16393,
16394,
16395,
16396,
16397,
16398,
16399,
16400,
16401,
16402,
16403,
16404,
16405,
16406,
16407,
16408,
16409,
16410,
16411,
16412,
16413,
16414,
16415,
16416,
16417,
16418,
16419,
16420,
16421,
16422,
16423,
16424,
16425,
16426,
16427,
17083,
17084,
17328,
17432,
17440,
17491,
18170,
19776,
19812,
22693,
23013,
25280,
29130,
29131,
29132,
29133,
29134,
29135,
29136,
29137,
29138,
29139,
29140,
29141,
29676,
29677,
29678,
29679,
29680,
29681,
29682,
29683,
29684,
29685,
29686,
29687,
29688,
29689,
29690,
29691,
29692,
29693,
29694,
29695,
29696,
29697,
29893,
29894,
29895,
29896,
29897,
29898,
29899,
29900,
29901,
29902,
29903,
29904,
29905,
29906,
29907,
29908,
29909,
30165,
33957,
35755,
35756,
35757,
35758,
35759,
35760,
35761,
35762,
35763,
35764,
35765,
35766,
35767,
35768,
35769,
35770,
35771,
35772,
35773,
35774,
35775,
35776,
35777,
35778,
35779,
35780,
35781,
35782,
35783,
35784,
35785,
35786,
35787,
35788,
35789,
35790,
35791,
35792,
35793,
35794,
35795,
35796,
35797,
35798,
35799,
35800,
35801,
35802,
35803,
35804,
35805,
35806,
35807,
35808,
35809,
35810,
35811,
35812,
35813,
35814,
36081,
36082,
36083,
36116,
36145,
36146,
37274,
37275,
37276,
37277,
37278,
37279,
37280,
37365,
37367,
37368,
38175,
38176,
40110,
40829,
40830,
40831,
43074,
43083,
43090,
43569,
44431,
45351,
45511,
46535,
46536,
46537,
46538,
46539,
46540,
46541,
46542,
46544,
46545,
46546,
46547,
46549,
47030,
47031,
47032,
47489,
48042,
51155,
53126,
54166,
55573,
58665,
58785,
58881,
58991,
59397,
59398,
59399,
59713,
61393,
61409,
62840,
62841,
62842,
62843,
62844,
62845,
62846,
62847,
62848,
62849,
62850,
62851,
62852,
62853,
62854,
62855,
62856,
62857,
62858,
62859,
62860,
62861,
62862,
62863,
63945,
65780,
66074,
69245,
69246,
69965,
74290,
74973,
74974,
74975,
75690,
79504,
82813,
83733,
85613,
90965,
92627,
94992,
100151,
100155,
102756,
102768,
106128,
106515,
108525,
112276,
112997,
115791,
117605,
117797,
117851,
120948,
121437,
121485,
124927,
127363,
128446,
129088,
129210,
129211,
129212,
129213,
129214,
129215,
129216,
129217,
129218,
130934,
132927,
132992,
133843,
137237,
138000,
139571,
139825,
139826,
139875,
141458,
142214,
142215,
143259,
143260,
145271,
148659,
149264,
149265,
149266,
152185,
152186,
152187,
155442,
155898,
156314,
156433,
160980,
161322,
164090,
164673,
164675,
166762,
168338,
168464,
169305,
174941,
177403,
177404,
177405,
186593,
186948,
190097,
190292,
190316,
191214,
192147,
192471,
193409,
196370,
196371,
196372,
196373,
196374,
196377,
196378,
196379,
196382,
198694,
199091,
203410,
203411,
203412,
203413,
203414,
203415,
203416,
203417,
203418,
203419,
203439,
208272,
223102,
248486,
249070,
253624,
260207,
261888,
261890,
266812,
268637,
269338,
269339,
269340,
275584,
276415,
278647,
280606,
289440,
289519,
291441,
294255,
301264,
304027,
307453,
308570,
310738,
310740,
312225,
326282,
328599,
328600,
335841,
345894,
345895,
345896,
345897,
350875,
350878,
353475,
353478,
355269,
363033,
363034,
363035,
363036,
363039,
363040,
365351,
371139,
372556,
374177,
375165,
375451
        };

    }
}
