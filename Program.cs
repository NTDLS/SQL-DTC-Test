using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Transactions;
using System.Windows.Forms;

namespace SQLDTCTest
{
    static class Program
    {
        [STAThread]
        static void Main(string[] argv)
        {

            if (argv.Count() < 1)
            {
                Console.WriteLine("Usage: {0} <Server1> [<Server2>...]", System.AppDomain.CurrentDomain.FriendlyName);
                return;
            }

            DateTime StartDate = DateTime.Now;

            List<SqlConnection> sqlConnections = new List<SqlConnection>();

            try
            {
                foreach (string serverName in argv)
                {
                    SqlConnectionStringBuilder sqlConnectionString = new SqlConnectionStringBuilder();
                    sqlConnectionString.ApplicationName = "DTSTest v" + Application.ProductVersion;
                    sqlConnectionString.DataSource = serverName;
                    sqlConnectionString.InitialCatalog = "tempdb";
                    sqlConnectionString.IntegratedSecurity = true;

                    sqlConnections.Add(new SqlConnection(sqlConnectionString.ToString()));
                }

                var transactionOptions = new System.Transactions.TransactionOptions();
                transactionOptions.IsolationLevel = System.Transactions.IsolationLevel.ReadCommitted;
                transactionOptions.Timeout = new TimeSpan(1, 0, 0);

                WriteConsole("Beginning transaction scope for 1 hour.");
                using (TransactionScope transaction = new TransactionScope(System.Transactions.TransactionScopeOption.Required, transactionOptions))
                {
                    WriteConsole("Opening SQL connections.");
                    foreach (SqlConnection sqlConnection in sqlConnections)
                    {
                        SqlConnectionStringBuilder connectionString = new SqlConnectionStringBuilder(sqlConnection.ConnectionString);
                        WriteConsole("Opening SQL connection to [" + connectionString.DataSource + "].");
                        sqlConnection.Open();
                        WriteConsole("Success.");
                    }

                    WriteConsole("Beginning test executions.");
                    foreach (SqlConnection sqlConnection in sqlConnections)
                    {
                        SqlConnectionStringBuilder connectionString = new SqlConnectionStringBuilder(sqlConnection.ConnectionString);

                        WriteConsole("Creating test table on [" + connectionString.DataSource + "].");
                        ExecuteNonQuery(sqlConnection, "IF OBJECT_ID('SQLDTCTest') IS NOT NULL DROP TABLE [SQLDTCTest]");
                        ExecuteNonQuery(sqlConnection, "IF OBJECT_ID('SQLDTCTest') IS NULL CREATE TABLE [SQLDTCTest]([Value] int)");

                        WriteConsole("Inserting test record on [" + connectionString.DataSource + "].");
                        ExecuteNonQuery(sqlConnection, "INSERT INTO SQLDTCTest([Value]) SELECT TOP 100 object_id FROM sys.objects");

                        WriteConsole("Success on [" + connectionString.DataSource + "].");
                        System.Threading.Thread.Sleep(1000);
                    }

                    WriteConsole("Comitting transaction.");
                    transaction.Complete();
                } //using (TransactionScope transaction = new TransactionScope(System.Transactions.TransactionScopeOption.Required, transactionOptions))

                WriteConsole("Completed successfullt.");
            }
            catch (Exception ex)
            {
                WriteConsole(GetExceptionText(ex));
            }

            foreach (SqlConnection sqlConnection in sqlConnections)
            {
                if (sqlConnection.State == System.Data.ConnectionState.Open)
                {
                    SqlConnectionStringBuilder connectionString = new SqlConnectionStringBuilder(sqlConnection.ConnectionString);
                    WriteConsole("Closing SQL connection to [" + connectionString.DataSource + "].");
                    sqlConnection.Close();
                }
            }
        }

        private static void WriteConsole(string text)
        {
            DateTime now = DateTime.Now;
            Console.WriteLine(now.ToShortDateString() + " " + now.ToShortTimeString() + "> " + text);
        }

        private static string GetExceptionText(Exception ex)
        {
            return "[" + ex.Message + "]" + ((ex.InnerException != null && ex.InnerException.Message != null) ? ", Inner exception [" + ex.InnerException.Message + "]" : "");
        }

        public static void ExecuteNonQuery(SqlConnection connection, string sqlCommandText)
        {
            using (SqlCommand sqlCommand = new SqlCommand(sqlCommandText, connection))
            {
                sqlCommand.ExecuteNonQuery();
            }
        }
    }
}
