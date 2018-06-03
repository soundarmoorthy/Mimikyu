using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LoadPayerPlanDataToMongo
{
    public class ProviderSubSpecialityLoader
    {

        SqlConnection conn;
        SqlConnection conn1;
        Random random;

        public ProviderSubSpecialityLoader()
        {

            random = new Random(1);
        }



        public void Load()
        {

            using (conn = new SqlConnection("Data Source=.;Initial Catalog=CMR;Integrated Security=True"))
            {
                using (conn1 = new SqlConnection("Data Source=.;Initial Catalog=CMR;Integrated Security=True"))
                {
                    conn.Open();
                    conn1.Open();

                    SqlCommand cmd = new SqlCommand("select ProviderKey from provider.Provider", conn);
                    var rs = cmd.ExecuteReader();
                    ReadandUpdateSpecialities(rs);
                }
            }
        }

        private void ReadandUpdateSpecialities(SqlDataReader rs)
        {
            while (rs.Read())
            {
                //result.NextResult();
                var key = (int)rs.GetSqlInt32(0);
                if (key == 1)
                    continue;
                var primary = 1;
                foreach (var sp in Specialities())
                {
                    SqlCommand insertCmd = new SqlCommand($"insert into provider.PracticeSubSpecialty (Practice, SubSpecialty, IsPrimary, IsActive, CreatedBy) values ({key}, {sp},{primary}, 1,1)", conn1);
                    insertCmd.ExecuteNonQuery();
                    primary = 0;
                }
            }
        }

        private IEnumerable<int> Specialities()
        {
            var count = random.Next(6, 30);
            for (int i = 0; i < count; i++)
            {
                yield return random.Next(1, 252);
            }
        }
    }
}
