using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data;
using System.Data.SqlClient; //Assembly:System.Data.SqlClient.dll
using CsvHelper; 
using CsvHelper.Configuration;

namespace econnect
{
    class Econnect
    {
        static void Main(string[] args)
        {
            using (SqlConnection dbConnection = new SqlConnection(@"Data Source=na-sqlc03v03\sql3,4334;Initial Catalog=SingleCustomerView;Integrated Security=True;"))
            {
                dbConnection.Open();
                SqlCommand truncate1 = new SqlCommand("TRUNCATE TABLE dbo.[EMAIL_RN]", dbConnection);
                SqlCommand truncate2 = new SqlCommand("TRUNCATE TABLE dbo.[EMAIL_SUPP]", dbConnection);
                SqlCommand truncate3 = new SqlCommand("TRUNCATE TABLE dbo.[EMAIL_TNA]", dbConnection);
                truncate1.ExecuteNonQuery();
                truncate2.ExecuteNonQuery();
                truncate3.ExecuteNonQuery();
                using (SqlBulkCopy s = new SqlBulkCopy(dbConnection))
                {
                    List<dynamic> rows0;
                    List<string> columns0;
                    using (var reader0 = new StreamReader(@"Y:\Single Customer View\Data\Sources\Email_TNA_News.csv"))
                    using (var csv0 = new CsvReader(reader0, System.Globalization.CultureInfo.CurrentCulture)) // added CultureInfo.CurrentCulture
                    {
                        rows0 = csv0.GetRecords<dynamic>().ToList();
                        columns0 = csv0.Context.HeaderRecord.ToList();
                    }

                    if (rows0.Count == 0)
                        return;
                    var table0 = new DataTable();
                    s.ColumnMappings.Clear();
                    foreach (var c in columns0)
                    {
                        table0.Columns.Add(c);
                        s.ColumnMappings.Add(c, c);
                    }

                    foreach (IDictionary<string, object> row in rows0)
                    {
                        var rowValues = row.Values
                        .Select(a => string.IsNullOrEmpty(a.ToString()) ? null : a)
                        .ToArray();
                        table0.Rows.Add(rowValues);
                    }
                    s.DestinationTableName = "dbo.[EMAIL_TNA]";
                    s.WriteToServer(table0);
               
                    List<dynamic> rows1; 
                    List<string> columns1;
                    using (var reader1 = new StreamReader(@"Y:\Single Customer View\Data\Sources\Email_Research_News.csv"))
                    using (var csv1 = new CsvReader(reader1, System.Globalization.CultureInfo.CurrentCulture)) // added CultureInfo.CurrentCulture
                    {
                        rows1 = csv1.GetRecords<dynamic>().ToList();
                        columns1 = csv1.Context.HeaderRecord.ToList();
                    }

                    if (rows1.Count == 0)
                        return;
                    var table1 = new DataTable();
                    s.ColumnMappings.Clear();
                    foreach (var c in columns1)
                    {
                        table1.Columns.Add(c);
                        s.ColumnMappings.Add(c, c);
                    }

                    foreach (IDictionary<string, object> row in rows1)
                    {
                        var rowValues = row.Values
                        .Select(a => string.IsNullOrEmpty(a.ToString()) ? null : a)
                        .ToArray();
                        table1.Rows.Add(rowValues);
                    }
                    s.DestinationTableName = "dbo.[EMAIL_RN]";
                    s.WriteToServer(table1);

                    List<dynamic> rows2;
                    List<string> columns2;
                    using (var reader2 = new StreamReader(@"Y:\Single Customer View\Data\Sources\Email_Suppressions.csv"))
                    using (var csv2 = new CsvReader(reader2, System.Globalization.CultureInfo.CurrentCulture)) // added CultureInfo.CurrentCulture
                    {
                        rows2 = csv2.GetRecords<dynamic>().ToList();
                        columns2 = csv2.Context.HeaderRecord.ToList();
                    }

                    if (rows1.Count == 0)
                        return;
                    var table2 = new DataTable();
                    s.ColumnMappings.Clear();
                    foreach (var c in columns2)
                    {
                        table2.Columns.Add(c);
                        s.ColumnMappings.Add(c, c);
                    }

                    foreach (IDictionary<string, object> row in rows2)
                    {
                        var rowValues = row.Values
                        .Select(a => string.IsNullOrEmpty(a.ToString()) ? null : a)
                        .ToArray();
                        table2.Rows.Add(rowValues);
                    }
                    s.DestinationTableName = "dbo.[EMAIL_SUPP]";
                    s.WriteToServer(table2);
                }
            }
        }
    }
}
