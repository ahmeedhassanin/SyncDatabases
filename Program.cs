using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Npgsql;
using System.Data.SqlClient;

namespace SyncDatabases
{
    class Program
    {
        public static string student = "student";
        public static string addresses = "addresses";
        public static string Requests = "requests";
        public static string connectionString_Source = "";
        public static string connectionString_Destination = "";
        static void Main(string[] args)
        {
            string[] connectionStringPG = File.ReadAllLines(Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "connectionStringSource.txt"));
            string[] connectionStringSQL = File.ReadAllLines(Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "connectionStringDestination.txt"));
            connectionString_Source = connectionStringPG[0];
            connectionString_Destination = connectionStringSQL[0];
            SyncDatabase();
        }
        public static void SyncDatabase()
        {
            try
            { 
                SyncaddressesTable();
            } 
            catch (Exception ex) 
            { 
                Logs(ex.Message.ToString());
            }
            try 
            { 
                SyncRequestsTable();
            } 
            catch (Exception ex) 
            {
                Logs(ex.Message.ToString());
            }
        }
      
        public static void SyncaddressesTable()
        {
            NpgsqlCommand PGcommand;
            DataTable dataTable = new DataTable();
            string GetAdresses = "select * from "+ addresses + " where sync_status in (1,2)"; //where Status = 1
            using (NpgsqlConnection PGconnection = new NpgsqlConnection(connectionString_Source))
            {
                using (PGcommand = new NpgsqlCommand(GetAdresses, PGconnection))
                {
                    PGconnection.Open();
                    try
                    { 
                        dataTable.Load(PGcommand.ExecuteReader());
                    } 
                    catch (Exception ex)
                    {
                        Logs(ex.Message.ToString()); return;
                    }
                }

            }
            //BulkInsert(dataTable);
            if (dataTable.Rows.Count != 0)
            {
                string insert_Query = "";
                string UpdateSqlQuery = "update " + addresses + " set "
                           + "sync_status =" + "0 where id in (";
                foreach (DataRow row in dataTable.Rows)
                {
                    
                    string date = (String.IsNullOrEmpty(row["addeddate"].ToString()) ? "null" : "'" + DateTime.Parse(row["addeddate"].ToString().Split(' ')[0]).ToString("yyyy-MM-dd") + "'");
                    if ((String.IsNullOrEmpty(row["sync_status"].ToString()) ? "null" : row["sync_status"].ToString()) == "1")
                    {
                        insert_Query += " update " + addresses + " set "
                          + "property_number = " + (String.IsNullOrEmpty(row["property_number"].ToString()) ? "null" : row["property_number"].ToString()) + ","
                          + "floor_number = " + (String.IsNullOrEmpty(row["floor_number"].ToString()) ? "null" : row["floor_number"].ToString()) + ","
                          + "apartment_number = " + (String.IsNullOrEmpty(row["apartment_number"].ToString()) ? "null" : row["apartment_number"].ToString()) + ","
                          + "streetname = " + (String.IsNullOrEmpty(row["streetname"].ToString()) ? "null" : "'" + row["streetname"].ToString() + "'") + ","
                          + "unique_mark = " + (String.IsNullOrEmpty(row["unique_mark"].ToString()) ? "null" : "'" + row["unique_mark"].ToString() + "'") + ","
                          + "description = " + (String.IsNullOrEmpty(row["description"].ToString()) ? "null" : "'" + row["description"].ToString() + "'") + ","
                          + "districtid = " + (String.IsNullOrEmpty(row["districtid"].ToString()) ? "null" : row["districtid"].ToString()) + ","
                          + "requestid = " + (String.IsNullOrEmpty(row["requestid"].ToString()) ? "null" : row["requestid"].ToString()) + ","
                          + "addeddate  =" + (String.IsNullOrEmpty(row["addeddate"].ToString()) ? "null" : "'" + DateTime.Parse(row["addeddate"].ToString().Split(' ')[0]).ToString("yyyy-MM-dd") + "'") + ","
                          + "modifieddate =" + (String.IsNullOrEmpty(row["modifieddate"].ToString()) ? "null" : "'" + DateTime.Parse(row["modifieddate"].ToString().Split(' ')[0]).ToString("yyyy-MM-dd") + "'") + ","
                          + "createdby = " + (String.IsNullOrEmpty(row["createdby"].ToString()) ? "null" : "'" + row["createdby"].ToString() + "'") + ", "
                          + "updatedby = " + (String.IsNullOrEmpty(row["updatedby"].ToString()) ? "null" : "'" + row["updatedby"].ToString() + "'") + ", "
                          + "regionid = " + (String.IsNullOrEmpty(row["regionid"].ToString()) ? "null" : row["regionid"].ToString()) + ", "
                          + "sync_status = " + (String.IsNullOrEmpty(row["sync_status"].ToString()) ? "null" : row["sync_status"].ToString()) +" "
                          +"where id = "+(String.IsNullOrEmpty(row["id"].ToString()) ? "null" : row["id"].ToString()) + "; ";// + Environment.NewLine;

                    }
                   //     UpdateSqlQuery += (String.IsNullOrEmpty(row["id"].ToString()) ? "null" : row["id"].ToString()) + ", ";

                    else if ((String.IsNullOrEmpty(row["sync_status"].ToString()) ? "null" : row["sync_status"].ToString()) == "2")
                    {
                        insert_Query += " insert into " + addresses + " select "
                          + (String.IsNullOrEmpty(row["id"].ToString()) ? "null" : row["id"].ToString()) + ","
                          + (String.IsNullOrEmpty(row["property_number"].ToString()) ? "null" : row["property_number"].ToString()) + ","
                          + (String.IsNullOrEmpty(row["floor_number"].ToString()) ? "null" : row["floor_number"].ToString()) + ","
                          + (String.IsNullOrEmpty(row["apartment_number"].ToString()) ? "null" : row["apartment_number"].ToString()) + ","
                          + (String.IsNullOrEmpty(row["streetname"].ToString()) ? "null" : "'" + row["streetname"].ToString() + "'") + ","
                          + (String.IsNullOrEmpty(row["unique_mark"].ToString()) ? "null" : "'" + row["unique_mark"].ToString() + "'") + ","
                          + (String.IsNullOrEmpty(row["description"].ToString()) ? "null" : "'" + row["description"].ToString() + "'") + ","
                          + (String.IsNullOrEmpty(row["districtid"].ToString()) ? "null" : row["districtid"].ToString()) + ","
                          + (String.IsNullOrEmpty(row["requestid"].ToString()) ? "null" : row["requestid"].ToString()) + ","
                          + (String.IsNullOrEmpty(row["addeddate"].ToString()) ? "null" : "'" + DateTime.Parse(row["addeddate"].ToString().Split(' ')[0]).ToString("yyyy-MM-dd") + "'") + ","
                          + (String.IsNullOrEmpty(row["modifieddate"].ToString()) ? "null" : "'" + DateTime.Parse(row["modifieddate"].ToString().Split(' ')[0]).ToString("yyyy-MM-dd") + "'") + ","
                          + (String.IsNullOrEmpty(row["createdby"].ToString()) ? "null" : "'" + row["createdby"].ToString() + "'") + ","
                          + (String.IsNullOrEmpty(row["updatedby"].ToString()) ? "null" : "'" + row["updatedby"].ToString() + "'") + ","
                          + (String.IsNullOrEmpty(row["regionid"].ToString()) ? "null" : row["regionid"].ToString())+", "
                        +(String.IsNullOrEmpty(row["sync_status"].ToString()) ? "null" : row["sync_status"].ToString()) + " ; "
                         + Environment.NewLine;
                    }
                     UpdateSqlQuery += (String.IsNullOrEmpty(row["id"].ToString()) ? "null" : row["id"].ToString()) + ", ";
                }
                char endd = ')';
                StringBuilder sb = new StringBuilder(UpdateSqlQuery);
                sb[sb.Length - 2] = endd;
                
                UpdateSqlQuery = sb.ToString();

                using (NpgsqlConnection PGconnection = new NpgsqlConnection(connectionString_Destination))
                {
                    using (PGcommand = new NpgsqlCommand(insert_Query, PGconnection))
                    {
                        PGconnection.Open();
                        try
                        { 
                            PGcommand.ExecuteNonQuery(); 
                        } 
                        catch (Exception ex) 
                        { 
                            Logs(ex.Message.ToString()); return;
                        }
                        dataTable.Dispose();
                    }
                }
                using (NpgsqlConnection PGconnection = new NpgsqlConnection(connectionString_Source))
                {
                    using (PGcommand = new NpgsqlCommand(UpdateSqlQuery, PGconnection))
                    {
                        PGconnection.Open();
                        try
                        {
                            dataTable.Load(PGcommand.ExecuteReader());
                        }
                        catch (Exception ex)
                        {
                            Logs(ex.Message.ToString()); return;
                        }
                    }

                }
            }
            NpgsqlConnection.ClearAllPools();
            SqlConnection.ClearAllPools();
        }

        public static void SyncRequestsTable()
        {
            NpgsqlCommand PGcommand;
            // string connectionStringPG = Application.startuppath + "\\" + connectionStringSQL.txt
            DataTable dataTable = new DataTable();
            string get_new_Students = "select * from " + Requests + " where sync_status in (1,2)"; //where Status = 1
            using (NpgsqlConnection PGconnection = new NpgsqlConnection(connectionString_Source))
            {
                using (PGcommand = new NpgsqlCommand(get_new_Students, PGconnection))
                {
                    PGconnection.Open();
                    try { dataTable.Load(PGcommand.ExecuteReader()); } catch (Exception ex) { Logs(ex.Message.ToString()); return; }
                }
            }
            if (dataTable.Rows.Count != 0)
            {
                string insert_Query = "";
                string UpdateSqlQuery = "update " + Requests + " set "
                           + "sync_status =" + "0 where id in (";
                foreach (DataRow row in dataTable.Rows)
                {
                    if ((String.IsNullOrEmpty(row["sync_status"].ToString()) ? "null" : row["sync_status"].ToString()) == "1")
                    {
                        insert_Query += "update " + Requests + " set "
                          + "unittype = " + (String.IsNullOrEmpty(row["unittype"].ToString()) ? "null" : "'" + row["unittype"].ToString() + "'") + ","
                          + "requeststatus = " + (String.IsNullOrEmpty(row["requeststatus"].ToString()) ? "null" : row["requeststatus"].ToString()) + ","
                          + "location = " + (String.IsNullOrEmpty(row["location"].ToString()) ? "null" : "'" + row["location"].ToString() + "'") + ","
                          + "area = " + (String.IsNullOrEmpty(row["area"].ToString()) ? "null" : "'" + row["area"].ToString() + "'") + ","
                          + "price = " + (String.IsNullOrEmpty(row["price"].ToString()) ? "null" : row["price"].ToString()) + ","
                          + "receiptimagepath = " + (String.IsNullOrEmpty(row["receiptimagepath"].ToString()) ? "null" : "'" + row["receiptimagepath"].ToString() + "'") + ","
                          + "requestnumber = " + (String.IsNullOrEmpty(row["requestnumber"].ToString()) ? "null" : row["requestnumber"].ToString()) + ","
                          + "userid = '" + (String.IsNullOrEmpty(row["userid"].ToString()) ? "null" : row["userid"].ToString()) + "',"
                          + "addeddate = " + (String.IsNullOrEmpty(row["addeddate"].ToString()) ? "null" : "'" + DateTime.Parse(row["addeddate"].ToString().Split(' ')[0]).ToString("yyyy-MM-dd") + "'") + ","
                          + "modifieddate = " + (String.IsNullOrEmpty(row["modifieddate"].ToString()) ? "null" : "'" + DateTime.Parse(row["modifieddate"].ToString().Split(' ')[0]).ToString("yyyy-MM-dd") + "'") + ","
                          + "createdby = " + (String.IsNullOrEmpty(row["createdby"].ToString()) ? "null" : "'" + row["createdby"].ToString() + "'") + ", "
                          + "updatedby = " + (String.IsNullOrEmpty(row["updatedby"].ToString()) ? "null" : "'" + row["updatedby"].ToString() + "'") + ", "
                          + "sync_status = " + (String.IsNullOrEmpty(row["sync_status"].ToString()) ? "null" : row["sync_status"].ToString()) +" "
                          + "where id = " + (String.IsNullOrEmpty(row["id"].ToString()) ? "null" : row["id"].ToString()) + "; ";// + Environment.NewLine;
                    }
                    else if ((String.IsNullOrEmpty(row["sync_status"].ToString()) ? "null" : row["sync_status"].ToString()) == "2")
                    {
                        insert_Query += " insert into " + Requests + " select "
                          + (String.IsNullOrEmpty(row["id"].ToString()) ? "null" : row["id"].ToString())+", "
                          + (String.IsNullOrEmpty(row["unittype"].ToString()) ? "null" : "'" + row["unittype"].ToString() + "'") + ","
                          + (String.IsNullOrEmpty(row["requeststatus"].ToString()) ? "null" : row["requeststatus"].ToString()) + ","
                          + (String.IsNullOrEmpty(row["location"].ToString()) ? "null" : "'" + row["location"].ToString() + "'") + ","
                          + (String.IsNullOrEmpty(row["area"].ToString()) ? "null" : "'" + row["area"].ToString() + "'") + ","
                          + (String.IsNullOrEmpty(row["price"].ToString()) ? "null" : row["price"].ToString()) + ","
                          + (String.IsNullOrEmpty(row["receiptimagepath"].ToString()) ? "null" : "'" + row["receiptimagepath"].ToString() + "'") + ","
                          + (String.IsNullOrEmpty(row["requestnumber"].ToString()) ? "null" : row["requestnumber"].ToString()) + ",'"
                          + (String.IsNullOrEmpty(row["userid"].ToString()) ? "null" : row["userid"].ToString()) + "',"
                          + (String.IsNullOrEmpty(row["addeddate"].ToString()) ? "null" : "'"+ DateTime.Parse(row["addeddate"].ToString().Split(' ')[0]).ToString("yyyy-MM-dd") ) + "',"
                          + (String.IsNullOrEmpty(row["modifieddate"].ToString()) ? "null" : "'" + DateTime.Parse(row["modifieddate"].ToString().Split(' ')[0]).ToString("yyyy-MM-dd") + "'") + ","
                          + (String.IsNullOrEmpty(row["createdby"].ToString()) ? "null" : "'" + row["createdby"].ToString() + "'") + ","
                          + (String.IsNullOrEmpty(row["updatedby"].ToString()) ? "null" : "'" + row["updatedby"].ToString() + "'") + ","
                          //+ (String.IsNullOrEmpty(row["callcenter_note"].ToString()) ? "null" : "'" + row["callcenter_note"].ToString() + "'") + ","
                         // + (String.IsNullOrEmpty(row["currentstatus"].ToString()) ? "null" : "'" + row["currentstatus"].ToString() + "'") + ","
                         // + (String.IsNullOrEmpty(row["payment_confirm"].ToString()) ? "null" : "'" + row["payment_confirm"].ToString() + "'") + ","
                          //+ (String.IsNullOrEmpty(row["callconfirm"].ToString()) ? "null" : "'" + row["callconfirm"].ToString() + "'") + ", "
                          +"null,null,null,null,null,"+ (String.IsNullOrEmpty(row["sync_status"].ToString()) ? "null" : row["sync_status"].ToString()) + " ; "
                         + Environment.NewLine;
                    }
                    UpdateSqlQuery += (String.IsNullOrEmpty(row["id"].ToString()) ? "null" : row["id"].ToString()) + ", ";
                }
                char endd = ')';
                StringBuilder sb = new StringBuilder(UpdateSqlQuery);
                sb[sb.Length - 2] = endd;
                UpdateSqlQuery = sb.ToString();

                using (NpgsqlConnection PGconnection = new NpgsqlConnection(connectionString_Destination))
                {
                    using (PGcommand = new NpgsqlCommand(insert_Query, PGconnection))
                    {
                        PGconnection.Open();
                        try { PGcommand.ExecuteNonQuery(); } catch (Exception ex) { Logs(ex.Message.ToString()); return; }
                        dataTable.Dispose();
                    }
                }
                using (NpgsqlConnection PGconnection = new NpgsqlConnection(connectionString_Source))
                {
                    using (PGcommand = new NpgsqlCommand(UpdateSqlQuery, PGconnection))
                    {
                        PGconnection.Open();
                        try
                        {
                            dataTable.Load(PGcommand.ExecuteReader());
                        }
                        catch (Exception ex)
                        {
                            Logs(ex.Message.ToString()); return;
                        }
                    }

                }
            }
            NpgsqlConnection.ClearAllPools();
            SqlConnection.ClearAllPools();
        }
        public static void Logs(string ex)
        {
            // Create a writer and open the file:
            StreamWriter log;
            if (!File.Exists(Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "LogFile.txt")))
            {
                log = new StreamWriter(Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "LogFile.txt"));
            }
            else
            {
                log = File.AppendText(Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "LogFile.txt"));
            }
            // Write to the file:
            log.WriteLine(DateTime.Now);
            log.WriteLine(ex);
            log.WriteLine();

            // Close the stream:
            log.Close();
            return;
        }
    }
}
