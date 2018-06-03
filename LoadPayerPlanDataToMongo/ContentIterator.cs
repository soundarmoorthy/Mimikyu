using System;
using System.Collections.Generic;
using System.Linq;

namespace LoadPayerPlanDataToMongo
{
    //Not Thread safe as of now
    internal class ContentIterator
    {

        IReadOnlyList<int> list;
        int next;
        int count = 0;

        public ContentIterator(IReadOnlyList<int> list)
        {
            this.list = list;
            next = 0;
            count = list.Count;

        }

        public int Next()
        {
            if (!list.Any())
                throw new NullReferenceException();

            next++;
            return list[next % count];

        }

        static Random random = new Random();
        internal IEnumerable<int> RandomSubset(int percent = 25)
        {
            if (!list.Any())
                return Enumerable.Empty<int>();

            int min = 1;
            var max = random.Next(min, min + (count * percent/100));

            List<int> l = new List<int>();
            int i = 0;
            do
            {
                var r = random.Next(0, max);
                l.Add(list[r]);
                i++;
            }
            while (i <= max);

            return l.Distinct();
        }
    }
}