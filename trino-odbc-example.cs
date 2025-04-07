using System;
using System.Data;
using Trino.Odbc;

namespace TrinoOdbcExample
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Trino ODBC Driver Example");
            
            try
            {
                // Connection string format
                string connectionString = 
                    "Host=localhost;" +
                    "Port=8080;" +
                    "UseSSL=false;" +
                    "Catalog=tpch;" +
                    "Schema=tiny;" +
                    "User=trino;" +
                    "Password=;";
                
                using (var connection = new TrinoConnection(connectionString))
                {
                    // Open the connection
                    Console.WriteLine("Opening connection to Trino server...");
                    connection.Open();
                    Console.WriteLine("Connection successful!");
                    
                    // Simple query example
                    string sql = "SELECT * FROM nation LIMIT 5";
                    using (var command = new TrinoCommand(sql, connection))
                    {
                        Console.WriteLine($"Executing query: {sql}");
                        
                        using (var reader = command.ExecuteReader())
                        {
                            // Display column names
                            Console.WriteLine("\nResults:");
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                Console.Write($"{reader.GetName(i)}\t");
                            }
                            Console.WriteLine();
                            
                            // Display column separator
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                Console.Write("--------\t");
                            }
                            Console.WriteLine();
                            
                            // Display data
                            while (reader.Read())
                            {
                                for (int i = 0; i < reader.FieldCount; i++)
                                {
                                    Console.Write($"{reader.GetValue(i)}\t");
                                }
                                Console.WriteLine();
                            }
                        }
                    }
                    
                    // Parameterized query example
                    string paramSql = "SELECT * FROM customer WHERE nationkey = @nationKey LIMIT 10";
                    using (var command = new TrinoCommand(paramSql, connection))
                    {
                        // Add a parameter
                        var parameter = command.CreateParameter();
                        parameter.ParameterName = "nationKey";
                        parameter.Value = 1;
                        command.Parameters.Add(parameter);
                        
                        Console.WriteLine($"\nExecuting parameterized query: {paramSql} with @nationKey = {parameter.Value}");
                        
                        using (var reader = command.ExecuteReader())
                        {
                            // Display column names
                            Console.WriteLine("\nResults:");
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                Console.Write($"{reader.GetName(i)}\t");
                            }
                            Console.WriteLine();
                            
                            // Display data (first 3 rows)
                            int rowCount = 0;
                            while (reader.Read() && rowCount < 3)
                            {
                                for (int i = 0; i < reader.FieldCount; i++)
                                {
                                    Console.Write($"{reader.GetValue(i)}\t");
                                }
                                Console.WriteLine();
                                rowCount++;
                            }
                            
                            if (reader.HasRows && rowCount == 3)
                            {
                                Console.WriteLine("... (more rows)");
                            }
                        }
                    }
                    
                    // Using DataTable example
                    string tableSql = "SELECT n.nationkey, n.name, r.name as region " +
                                    "FROM nation n JOIN region r ON n.regionkey = r.regionkey " +
                                    "ORDER BY n.nationkey";
                    
                    Console.WriteLine($"\nFilling DataTable with query: {tableSql}");
                    
                    using (var adapter = new TrinoDataAdapter(tableSql, connection))
                    {
                        var dataTable = new DataTable("Nations");
                        adapter.Fill(dataTable);
                        
                        Console.WriteLine($"\nDataTable filled with {dataTable.Rows.Count} rows");
                        Console.WriteLine("\nFirst 5 rows:");
                        
                        // Print column headers
                        foreach (DataColumn column in dataTable.Columns)
                        {
                            Console.Write($"{column.ColumnName}\t");
                        }
                        Console.WriteLine();
                        
                        // Print first 5 rows
                        for (int i = 0; i < Math.Min(5, dataTable.Rows.Count); i++)
                        {
                            foreach (var item in dataTable.Rows[i].ItemArray)
                            {
                                Console.Write($"{item}\t");
                            }
                            Console.WriteLine();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
                if (ex.InnerException != null)
                {
                    Console.WriteLine($"Inner Error: {ex.InnerException.Message}");
                }
            }
            
            Console.WriteLine("\nPress any key to exit...");
            Console.ReadKey();
        }
    }
}
