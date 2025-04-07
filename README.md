# Trino ODBC Driver for .NET

A fully functional ODBC driver for connecting .NET applications to Trino (formerly PrestoSQL) servers using C#.

## Features

- Implements the standard DbProviderFactory pattern for .NET database connectivity
- Full support for Trino SQL dialect
- Parameterized queries
- Connection pooling
- Asynchronous data retrieval
- Support for various Trino data types
- Implementation of standard ADO.NET interfaces

## Installation

### NuGet Package

```
Install-Package Trino.Odbc
```

Or via the .NET CLI:

```
dotnet add package Trino.Odbc
```

### Manual Installation

1. Clone this repository
2. Build the solution
3. Reference the output DLL in your project

## Usage

### Basic Connection

```csharp
using Trino.Odbc;

// Create a connection
string connectionString = 
    "Host=your-trino-server;" +
    "Port=8080;" +
    "UseSSL=false;" +
    "Catalog=hive;" +
    "Schema=default;" +
    "User=trino;" +
    "Password=your-password;";

using (var connection = new TrinoConnection(connectionString))
{
    connection.Open();
    
    // Use the connection...
}
```

### Executing Queries

```csharp
string sql = "SELECT * FROM your_table LIMIT 1000";

using (var command = new TrinoCommand(sql, connection))
using (var reader = command.ExecuteReader())
{
    while (reader.Read())
    {
        // Access data by index
        var id = reader.GetInt32(0);
        var name = reader.GetString(1);
        
        // Or by column name
        var value = reader["column_name"];
        
        // Process the data...
    }
}
```

### Using Parameters

```csharp
string sql = "SELECT * FROM orders WHERE customer_id = @customerId";

using (var command = new TrinoCommand(sql, connection))
{
    // Add parameters
    command.Parameters.Add(new TrinoParameter
    {
        ParameterName = "customerId",
        Value = 12345
    });
    
    using (var reader = command.ExecuteReader())
    {
        // Process results...
    }
}
```

### Using DataAdapter with DataSet/DataTable

```csharp
string sql = "SELECT * FROM customers WHERE region = @region";

using (var adapter = new TrinoDataAdapter(sql, connection))
{
    // Add parameters
    adapter.SelectCommand.Parameters.Add(new TrinoParameter
    {
        ParameterName = "region",
        Value = "EAST"
    });
    
    // Fill a DataTable
    var dataTable = new DataTable();
    adapter.Fill(dataTable);
    
    // Process the DataTable...
}
```

### Executing Non-Query Commands

```csharp
string sql = "INSERT INTO log_events (event_time, message) VALUES (@time, @message)";

using (var command = new TrinoCommand(sql, connection))
{
    command.Parameters.Add(new TrinoParameter { ParameterName = "time", Value = DateTime.Now });
    command.Parameters.Add(new TrinoParameter { ParameterName = "message", Value = "System event" });
    
    int rowsAffected = command.ExecuteNonQuery();
}
```

### Executing Scalar Queries

```csharp
string sql = "SELECT COUNT(*) FROM large_table";

using (var command = new TrinoCommand(sql, connection))
{
    long count = Convert.ToInt64(command.ExecuteScalar());
}
```

## Connection String Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| Host      | Trino server hostname or IP address | localhost |
| Port      | Trino server port | 8080 |
| UseSSL    | Whether to use HTTPS instead of HTTP | false |
| Catalog   | Trino catalog to use | (empty) |
| Schema    | Schema within the catalog | (empty) |
| User      | Username for authentication | anonymous |
| Password  | Password for authentication | (empty) |
| Timeout   | Connection timeout in seconds | 30 |

> **Note:** For backward compatibility, you can also use `Server=http://hostname:port` instead of separate Host/Port/UseSSL parameters.

## Limitations

- Trino doesn't support traditional ACID transactions, so the transaction methods are implemented as no-ops
- Limited support for prepared statements since Trino handles them differently than traditional databases
- The driver implements parameter substitution client-side

## Troubleshooting

If you encounter issues with the Trino ODBC driver, consider these common solutions:

### Connection Issues

1. **Can't connect to server**:
   - Verify that the host and port are correct
   - Check if SSL is required (UseSSL=true)
   - Make sure there's network connectivity to the Trino server
   - Try connecting with a browser to the Trino UI (usually at http://host:port)

2. **Authentication failures**:
   - Verify username and password
   - Check if the Trino server requires authentication
   - For Kerberos or other authentication methods, additional configuration may be needed

3. **Cannot find catalog/schema**:
   - Verify that the catalog and schema exist on the Trino server
   - Check if the user has permission to access the specified catalog/schema

### Query Issues

1. **No data returning**:
   - Try a simple query like `SELECT 1 AS test` to test connectivity
   - Use the included troubleshooting utility to test direct HTTP access to the Trino REST API
   - Check if there are any error messages in the Trino server logs

2. **Performance issues**:
   - Consider using smaller result sets with LIMIT clauses
   - Check if the Trino server is under heavy load
   - Optimize your queries to use appropriate filters

3. **Error handling**:
   - Use try/catch blocks to capture detailed error information
   - Enable debugging by adding console output in the ProcessResponseData method

### Enabling Debug Output

To enable additional debug output for troubleshooting, uncomment the debug Console.WriteLine lines in the driver source code.

## Performance Considerations

- For large result sets, consider using pagination in your queries (OFFSET/LIMIT)
- Use appropriate data types to avoid unnecessary conversions
- Consider creating views in Trino for complex queries that are used frequently

## License

MIT

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
