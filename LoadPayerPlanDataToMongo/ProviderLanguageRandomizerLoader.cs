using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LoadPayerPlanDataToMongo
{
    public class ProviderLanguageRandomizerLoader
    {

        SqlConnection conn;
        SqlConnection conn1;
        Random random;

        public ProviderLanguageRandomizerLoader()
        {
            random = new Random(1);

        }


        public void Load()
        {
            using (conn = new SqlConnection("Data Source=.;Initial Catalog=ReferralNetwork;Integrated Security=True"))
            {

                using (conn1 = new SqlConnection("Data Source=.;Initial Catalog=ReferralNetwork;Integrated Security=True"))
                {
                    conn.Open();
                    conn1.Open();

                    try
                    {
                        SqlCommand cmd = new SqlCommand("select ProviderKey from provider.Provider", conn);
                        var rs = cmd.ExecuteReader();
                        while (rs.Read())
                        {
                            //result.NextResult();
                            var key = (int)rs.GetSqlInt32(0);
                            if (key == 1)
                                continue;
                            foreach (var lang in GetRandomLanguages().Distinct())
                            {
                                try
                                {
                                    SqlCommand insertCmd = new SqlCommand($"insert into provider.ProviderLanguage (Provider, Language, IsActive, CreatedBy) values ({key}, {lang}, 1,1)", conn1);
                                    insertCmd.ExecuteNonQuery();
                                }
                                catch (Exception e)
                                {
                                    Console.WriteLine(e.ToString());
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.ToString());
                    }
                }
            }
        }

        private IEnumerable<int> GetRandomLanguages()
        {
            var count = random.Next(10);
            for(int i=0;i<count;i++)
            {
                yield return random.Next(1, 18);
            }

            yield return 4;
        }
    }
}
