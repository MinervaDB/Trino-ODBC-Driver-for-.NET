using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Trino.Odbc
{
    /// <summary>
    /// Main driver class for the Trino ODBC driver.
    /// </summary>
    public class TrinoOdbcDriver : DbProviderFactory, IDisposable
    {
        private static readonly HttpClient _httpClient = new HttpClient();
        public static readonly TrinoOdbcDriver Instance = new TrinoOdbcDriver();

        public override DbConnection CreateConnection() => new TrinoConnection();
        public override DbCommand CreateCommand() => new TrinoCommand();
        public override DbConnectionStringBuilder CreateConnectionStringBuilder() => new TrinoConnectionStringBuilder();
        public override DbParameter CreateParameter() => new TrinoParameter();
        public override DbDataAdapter CreateDataAdapter() => new TrinoDataAdapter();

        public void Dispose()
        {
            _httpClient.Dispose();
        }
    }

    /// <summary>
    /// Connection class for Trino database connections.
    /// </summary>
    public class TrinoConnection : DbConnection
    {
        private ConnectionState _state = ConnectionState.Closed;
        private string _connectionString;
        private TrinoConnectionStringBuilder _connectionStringBuilder;
        private string _database = string.Empty;
        private HttpClient _httpClient;
        private string _sessionToken;

        public TrinoConnection()
        {
            _httpClient = new HttpClient();
        }

        public TrinoConnection(string connectionString)
            : this()
        {
            ConnectionString = connectionString;
        }

        public override string ConnectionString
        {
            get => _connectionString;
            set
            {
                _connectionString = value;
                _connectionStringBuilder = new TrinoConnectionStringBuilder(value);
            }
        }

        public override string Database => _database;

        public override ConnectionState State => _state;

        public override string DataSource => _connectionStringBuilder?.Server ?? string.Empty;

        public override string ServerVersion => "Trino ODBC Driver 1.0";

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            // Trino doesn't support traditional transactions, so we'll implement a lightweight version
            return new TrinoTransaction(this, isolationLevel);
        }

        public override void Close()
        {
            if (_state != ConnectionState.Closed)
            {
                // Close any active connection and clear the session token
                _sessionToken = null;
                _state = ConnectionState.Closed;
            }
        }

        public override void ChangeDatabase(string databaseName)
        {
            if (_state != ConnectionState.Open)
            {
                throw new InvalidOperationException("Connection is not open");
            }

            _database = databaseName;
            // In Trino, we can just update the catalog/schema context
        }

        public override void Open()
        {
            if (_state == ConnectionState.Open)
                return;

            if (_connectionStringBuilder == null)
                throw new InvalidOperationException("Connection string has not been set");

            try
            {
                // Configure HTTP client
                _httpClient.DefaultRequestHeaders.Clear();
                _httpClient.DefaultRequestHeaders.Add("User-Agent", "Trino ODBC Driver .NET");
                _httpClient.DefaultRequestHeaders.Add("X-Trino-User", _connectionStringBuilder.User);
                
                // Add Basic Auth only if both username and password are provided and not empty
                if (!string.IsNullOrEmpty(_connectionStringBuilder.User) && 
                    !string.IsNullOrEmpty(_connectionStringBuilder.Password))
                {
                    string authValue = Convert.ToBase64String(
                        Encoding.UTF8.GetBytes($"{_connectionStringBuilder.User}:{_connectionStringBuilder.Password}"));
                    _httpClient.DefaultRequestHeaders.Add("Authorization", $"Basic {authValue}");
                }

                // Test connection with a simple query
                string protocol = _connectionStringBuilder.UseSSL ? "https" : "http";
                var baseUri = new Uri($"{protocol}://{_connectionStringBuilder.Host}:{_connectionStringBuilder.Port}/v1/statement");
                var content = new StringContent("SELECT 1", Encoding.UTF8, "application/json");
                var response = _httpClient.PostAsync(baseUri, content).Result;
                
                if (!response.IsSuccessStatusCode)
                {
                    throw new Exception($"Failed to connect to Trino server: {response.StatusCode}");
                }

                var responseContent = response.Content.ReadAsStringAsync().Result;
                var jsonResponse = JsonSerializer.Deserialize<Dictionary<string, object>>(responseContent);
                
                // Extract session information if available
                if (jsonResponse.ContainsKey("id"))
                {
                    _sessionToken = jsonResponse["id"].ToString();
                }

                _state = ConnectionState.Open;
                _database = _connectionStringBuilder.Catalog;
            }
            catch (Exception ex)
            {
                throw new DbException($"Failed to open connection to Trino server: {ex.Message}", ex);
            }
        }

        protected override DbCommand CreateDbCommand()
        {
            return new TrinoCommand { Connection = this };
        }

        public override void Dispose()
        {
            Close();
            _httpClient?.Dispose();
            base.Dispose();
        }
    }

    /// <summary>
    /// Connection string builder for Trino connections.
    /// </summary>
    public class TrinoConnectionStringBuilder : DbConnectionStringBuilder
    {
        public TrinoConnectionStringBuilder() { }

        public TrinoConnectionStringBuilder(string connectionString)
        {
            ConnectionString = connectionString;
        }

        public string Host
        {
            get => TryGetValue("Host", out object value) ? (string)value : "localhost";
            set => this["Host"] = value;
        }

        public int Port
        {
            get => TryGetValue("Port", out object value) && int.TryParse(value.ToString(), out int result) ? result : 8080;
            set => this["Port"] = value;
        }

        public bool UseSSL
        {
            get => TryGetValue("UseSSL", out object value) && bool.TryParse(value.ToString(), out bool result) && result;
            set => this["UseSSL"] = value;
        }

        public string Catalog
        {
            get => TryGetValue("Catalog", out object value) ? (string)value : string.Empty;
            set => this["Catalog"] = value;
        }

        public string Schema
        {
            get => TryGetValue("Schema", out object value) ? (string)value : string.Empty;
            set => this["Schema"] = value;
        }

        public string User
        {
            get => TryGetValue("User", out object value) ? (string)value : "anonymous";
            set => this["User"] = value;
        }

        public string Password
        {
            get => TryGetValue("Password", out object value) ? (string)value : string.Empty;
            set => this["Password"] = value;
        }

        public int Timeout
        {
            get => TryGetValue("Timeout", out object value) && int.TryParse(value.ToString(), out int result) ? result : 30;
            set => this["Timeout"] = value;
        }

        // For backward compatibility
        public string Server
        {
            get
            {
                string protocol = UseSSL ? "https" : "http";
                return $"{protocol}://{Host}:{Port}";
            }
            set
            {
                if (string.IsNullOrEmpty(value))
                    return;

                try
                {
                    var uri = new Uri(value);
                    UseSSL = uri.Scheme.Equals("https", StringComparison.OrdinalIgnoreCase);
                    Host = uri.Host;
                    Port = uri.Port;
                }
                catch
                {
                    // If unable to parse as URI, use as-is for Host
                    Host = value;
                }
            }
        }
    }

    /// <summary>
    /// Command implementation for executing Trino queries.
    /// </summary>
    public class TrinoCommand : DbCommand
    {
        private TrinoConnection _connection;
        private string _commandText;
        private int _commandTimeout = 30;
        private CommandType _commandType = CommandType.Text;
        private readonly List<TrinoParameter> _parameters = new List<TrinoParameter>();
        private TrinoTransaction _transaction;
        private bool _designTimeVisible = true;

        public override string CommandText
        {
            get => _commandText;
            set => _commandText = value;
        }

        public override int CommandTimeout
        {
            get => _commandTimeout;
            set => _commandTimeout = value;
        }

        public override CommandType CommandType
        {
            get => _commandType;
            set
            {
                if (value != CommandType.Text)
                    throw new NotSupportedException("Only CommandType.Text is supported for Trino");
                _commandType = value;
            }
        }

        public override UpdateRowSource UpdatedRowSource
        {
            get => UpdateRowSource.None;
            set => throw new NotSupportedException("UpdatedRowSource is not supported for Trino");
        }

        protected override DbConnection DbConnection
        {
            get => _connection;
            set => _connection = value as TrinoConnection;
        }

        protected override DbParameterCollection DbParameterCollection => new TrinoParameterCollection(_parameters);

        protected override DbTransaction DbTransaction
        {
            get => _transaction;
            set => _transaction = value as TrinoTransaction;
        }

        public override bool DesignTimeVisible
        {
            get => _designTimeVisible;
            set => _designTimeVisible = value;
        }

        public override void Cancel()
        {
            throw new NotImplementedException("Cancel operation is not implemented");
        }

        public override int ExecuteNonQuery()
        {
            using (var reader = ExecuteDbDataReader(CommandBehavior.Default))
            {
                int rowsAffected = 0;
                while (reader.Read())
                {
                    rowsAffected++;
                }
                return rowsAffected;
            }
        }

        public override object ExecuteScalar()
        {
            using (var reader = ExecuteDbDataReader(CommandBehavior.SingleRow))
            {
                if (reader.Read() && reader.FieldCount > 0)
                {
                    return reader.GetValue(0);
                }
                return null;
            }
        }

        public override void Prepare()
        {
            // Trino doesn't support prepared statements in the same way as traditional databases
            // This method is a no-op
        }

        protected override DbParameter CreateDbParameter()
        {
            var parameter = new TrinoParameter();
            _parameters.Add(parameter);
            return parameter;
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            if (_connection == null)
                throw new InvalidOperationException("Connection not set for command");

            if (_connection.State != ConnectionState.Open)
                throw new InvalidOperationException("Connection is not open");

            if (string.IsNullOrWhiteSpace(_commandText))
                throw new InvalidOperationException("Command text is empty");

            try
            {
                // Apply parameters to the SQL command
                string sql = ApplyParameters(_commandText);

                // Create a new HTTP client for this query
                using (var httpClient = new HttpClient())
                {
                    // Configure HTTP client with connection details
                    var connectionBuilder = new TrinoConnectionStringBuilder(_connection.ConnectionString);
                    httpClient.DefaultRequestHeaders.Add("User-Agent", "Trino ODBC Driver .NET");
                    httpClient.DefaultRequestHeaders.Add("X-Trino-User", connectionBuilder.User);
                    
                    if (!string.IsNullOrEmpty(connectionBuilder.Password))
                    {
                        string authValue = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{connectionBuilder.User}:{connectionBuilder.Password}"));
                        httpClient.DefaultRequestHeaders.Add("Authorization", $"Basic {authValue}");
                    }

                    // Set the catalog and schema if provided
                    if (!string.IsNullOrEmpty(connectionBuilder.Catalog))
                    {
                        httpClient.DefaultRequestHeaders.Add("X-Trino-Catalog", connectionBuilder.Catalog);
                    }

                    if (!string.IsNullOrEmpty(connectionBuilder.Schema))
                    {
                        httpClient.DefaultRequestHeaders.Add("X-Trino-Schema", connectionBuilder.Schema);
                    }

                    // Execute the query
                    string protocol = connectionBuilder.UseSSL ? "https" : "http";
                    var baseUri = new Uri($"{protocol}://{connectionBuilder.Host}:{connectionBuilder.Port}/v1/statement");
                    var content = new StringContent(sql, Encoding.UTF8, "application/json");
                    var response = httpClient.PostAsync(baseUri, content).Result;

                    if (!response.IsSuccessStatusCode)
                    {
                        var errorContent = response.Content.ReadAsStringAsync().Result;
                        throw new DbException($"Query execution failed: {response.StatusCode} - {errorContent}");
                    }

                    var responseContent = response.Content.ReadAsStringAsync().Result;
                    return new TrinoDataReader(httpClient, responseContent, connectionBuilder.Server);
                }
            }
            catch (Exception ex)
            {
                throw new DbException($"Error executing query: {ex.Message}", ex);
            }
        }

        private string ApplyParameters(string sql)
        {
            string result = sql;
            
            // Replace parameters in the query
            foreach (TrinoParameter param in _parameters)
            {
                string paramName = param.ParameterName;
                string paramValue = GetParameterValueString(param.Value);
                
                // Support both :param and @param syntax
                result = result.Replace(":" + paramName, paramValue);
                result = result.Replace("@" + paramName, paramValue);
            }
            
            return result;
        }

        private string GetParameterValueString(object value)
        {
            if (value == null || value == DBNull.Value)
                return "NULL";

            switch (value)
            {
                case string strValue:
                    return $"'{strValue.Replace("'", "''")}'";
                case DateTime dateTime:
                    return $"TIMESTAMP '{dateTime:yyyy-MM-dd HH:mm:ss}'";
                case bool boolValue:
                    return boolValue ? "TRUE" : "FALSE";
                default:
                    return value.ToString();
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                Close();
            }
            
            base.Dispose(disposing);
        }
    }

    /// <summary>
    /// DataAdapter implementation for Trino.
    /// </summary>
    public class TrinoDataAdapter : DbDataAdapter
    {
        private TrinoCommand _selectCommand;
        private TrinoCommand _insertCommand;
        private TrinoCommand _updateCommand;
        private TrinoCommand _deleteCommand;

        public TrinoDataAdapter() { }

        public TrinoDataAdapter(TrinoCommand selectCommand)
        {
            SelectCommand = selectCommand;
        }

        public TrinoDataAdapter(string selectCommandText, TrinoConnection connection)
        {
            SelectCommand = new TrinoCommand(selectCommandText, connection);
        }

        public new TrinoCommand SelectCommand
        {
            get => _selectCommand;
            set
            {
                _selectCommand = value;
                base.SelectCommand = value;
            }
        }

        public new TrinoCommand InsertCommand
        {
            get => _insertCommand;
            set
            {
                _insertCommand = value;
                base.InsertCommand = value;
            }
        }

        public new TrinoCommand UpdateCommand
        {
            get => _updateCommand;
            set
            {
                _updateCommand = value;
                base.UpdateCommand = value;
            }
        }

        public new TrinoCommand DeleteCommand
        {
            get => _deleteCommand;
            set
            {
                _deleteCommand = value;
                base.DeleteCommand = value;
            }
        }
    }

    /// <summary>
    /// Extended methods for the TrinoCommand class.
    /// </summary>
    public partial class TrinoCommand
    {
        public TrinoCommand() { }

        public TrinoCommand(string commandText)
        {
            CommandText = commandText;
        }

        public TrinoCommand(string commandText, TrinoConnection connection)
        {
            CommandText = commandText;
            Connection = connection;
        }

        public new TrinoConnection Connection
        {
            get => (TrinoConnection)base.DbConnection;
            set => base.DbConnection = value;
        }

        public new TrinoParameterCollection Parameters => 
            (TrinoParameterCollection)base.DbParameterCollection;

        public new TrinoTransaction Transaction
        {
            get => (TrinoTransaction)base.DbTransaction;
            set => base.DbTransaction = value;
        }
    }

    /// <summary>
    /// Custom exception class for Trino ODBC driver.
    /// </summary>
    public class DbException : Exception
    {
        public DbException(string message) : base(message) { }
        
        public DbException(string message, Exception innerException) 
            : base(message, innerException) { }
    }
}

    /// <summary>
    /// Parameter implementation for Trino queries.
    /// </summary>
    public class TrinoParameter : DbParameter
    {
        private string _parameterName;
        private object _value;
        private ParameterDirection _direction = ParameterDirection.Input;
        private bool _isNullable;
        private DbType _dbType = DbType.String;
        private int _size;
        private string _sourceColumn;
        private bool _sourceColumnNullMapping;
        private DataRowVersion _sourceVersion = DataRowVersion.Current;
        
        public override DbType DbType
        {
            get => _dbType;
            set => _dbType = value;
        }

        public override ParameterDirection Direction
        {
            get => _direction;
            set
            {
                if (value != ParameterDirection.Input)
                    throw new NotSupportedException("Only ParameterDirection.Input is supported for Trino");
                _direction = value;
            }
        }

        public override bool IsNullable
        {
            get => _isNullable;
            set => _isNullable = value;
        }

        public override string ParameterName
        {
            get => _parameterName;
            set => _parameterName = value?.TrimStart('@', ':');
        }

        public override int Size
        {
            get => _size;
            set => _size = value;
        }

        public override string SourceColumn
        {
            get => _sourceColumn;
            set => _sourceColumn = value;
        }

        public override bool SourceColumnNullMapping
        {
            get => _sourceColumnNullMapping;
            set => _sourceColumnNullMapping = value;
        }

        public override object Value
        {
            get => _value;
            set => _value = value;
        }

        public override DataRowVersion SourceVersion
        {
            get => _sourceVersion;
            set => _sourceVersion = value;
        }

        public override void ResetDbType()
        {
            _dbType = DbType.String;
        }
    }

    /// <summary>
    /// Parameter collection for Trino commands.
    /// </summary>
    public class TrinoParameterCollection : DbParameterCollection
    {
        private readonly List<TrinoParameter> _parameters;

        public TrinoParameterCollection(List<TrinoParameter> parameters)
        {
            _parameters = parameters;
        }

        public override int Count => _parameters.Count;

        public override object SyncRoot => ((ICollection)_parameters).SyncRoot;

        public override int Add(object value)
        {
            if (value is TrinoParameter parameter)
            {
                _parameters.Add(parameter);
                return _parameters.Count - 1;
            }
            throw new InvalidCastException("Parameter is not a TrinoParameter");
        }

        public override void AddRange(Array values)
        {
            foreach (var value in values)
            {
                Add(value);
            }
        }

        public override void Clear()
        {
            _parameters.Clear();
        }

        public override bool Contains(object value)
        {
            return value is TrinoParameter parameter && _parameters.Contains(parameter);
        }

        public override bool Contains(string value)
        {
            return IndexOf(value) != -1;
        }

        public override void CopyTo(Array array, int index)
        {
            ((ICollection)_parameters).CopyTo(array, index);
        }

        public override IEnumerator GetEnumerator()
        {
            return _parameters.GetEnumerator();
        }

        public override int IndexOf(object value)
        {
            if (value is TrinoParameter parameter)
            {
                return _parameters.IndexOf(parameter);
            }
            return -1;
        }

        public override int IndexOf(string parameterName)
        {
            for (int i = 0; i < _parameters.Count; i++)
            {
                if (string.Equals(_parameters[i].ParameterName, parameterName, StringComparison.OrdinalIgnoreCase))
                {
                    return i;
                }
            }
            return -1;
        }

        public override void Insert(int index, object value)
        {
            if (value is TrinoParameter parameter)
            {
                _parameters.Insert(index, parameter);
                return;
            }
            throw new InvalidCastException("Parameter is not a TrinoParameter");
        }

        public override void Remove(object value)
        {
            if (value is TrinoParameter parameter)
            {
                _parameters.Remove(parameter);
                return;
            }
            throw new InvalidCastException("Parameter is not a TrinoParameter");
        }

        public override void RemoveAt(int index)
        {
            _parameters.RemoveAt(index);
        }

        public override void RemoveAt(string parameterName)
        {
            int index = IndexOf(parameterName);
            if (index >= 0)
            {
                RemoveAt(index);
            }
        }

        protected override DbParameter GetParameter(int index)
        {
            return _parameters[index];
        }

        protected override DbParameter GetParameter(string parameterName)
        {
            int index = IndexOf(parameterName);
            if (index >= 0)
            {
                return _parameters[index];
            }
            throw new IndexOutOfRangeException($"Parameter '{parameterName}' not found");
        }

        protected override void SetParameter(int index, DbParameter value)
        {
            if (value is TrinoParameter parameter)
            {
                _parameters[index] = parameter;
                return;
            }
            throw new InvalidCastException("Parameter is not a TrinoParameter");
        }

        protected override void SetParameter(string parameterName, DbParameter value)
        {
            int index = IndexOf(parameterName);
            if (index >= 0)
            {
                SetParameter(index, value);
                return;
            }
            throw new IndexOutOfRangeException($"Parameter '{parameterName}' not found");
        }
    }

    /// <summary>
    /// Transaction implementation for Trino.
    /// </summary>
    public class TrinoTransaction : DbTransaction
    {
        private readonly TrinoConnection _connection;
        private readonly IsolationLevel _isolationLevel;

        public TrinoTransaction(TrinoConnection connection, IsolationLevel isolationLevel)
        {
            _connection = connection;
            _isolationLevel = isolationLevel;
        }

        public override IsolationLevel IsolationLevel => _isolationLevel;

        protected override DbConnection DbConnection => _connection;

        public override void Commit()
        {
            // Trino doesn't support traditional transactions
            // This is a no-op implementation
        }

        public override void Rollback()
        {
            // Trino doesn't support traditional transactions
            // This is a no-op implementation
        }
    }

    /// <summary>
    /// DataReader implementation for Trino query results.
    /// </summary>
    public class TrinoDataReader : DbDataReader
    {
        private readonly HttpClient _httpClient;
        private string _nextUri;
        private readonly string _serverBase;
        private JsonDocument _currentResult;
        private JsonElement.ArrayEnumerator _rowsEnumerator;
        private Dictionary<string, int> _columnMapping;
        private string[] _columnNames;
        private string[] _columnTypes;
        private bool _closed;
        private bool _hasRows;
        private int _currentRow = -1;

        public TrinoDataReader(HttpClient httpClient, string initialResponse, string serverBase)
        {
            _httpClient = httpClient;
            _serverBase = serverBase;
            _closed = false;

            // Parse the initial response
            ProcessResponseData(initialResponse);
            
            // If there's more data, fetch it
            if (!string.IsNullOrEmpty(_nextUri))
            {
                FetchNextBatch();
            }
        }

        private void ProcessResponseData(string responseJson)
        {
            try
            {
                Console.WriteLine($"DEBUG: Processing response: {responseJson.Substring(0, Math.Min(responseJson.Length, 200))}...");
                
                _currentResult = JsonDocument.Parse(responseJson);
                var root = _currentResult.RootElement;

                // Extract next URI if available
                if (root.TryGetProperty("nextUri", out JsonElement nextUriElement) && 
                    nextUriElement.ValueKind != JsonValueKind.Null)
                {
                    _nextUri = nextUriElement.GetString();
                    Console.WriteLine($"DEBUG: Next URI found: {_nextUri}");
                }
                else
                {
                    _nextUri = null;
                    Console.WriteLine("DEBUG: No next URI found");
                }

                // Extract column information if available
                if (root.TryGetProperty("columns", out JsonElement columnsElement) && 
                    columnsElement.ValueKind == JsonValueKind.Array)
                {
                    int columnCount = columnsElement.GetArrayLength();
                    _columnNames = new string[columnCount];
                    _columnTypes = new string[columnCount];
                    _columnMapping = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

                    Console.WriteLine($"DEBUG: Found {columnCount} columns");

                    for (int i = 0; i < columnCount; i++)
                    {
                        var column = columnsElement[i];
                        string name = column.GetProperty("name").GetString();
                        string type = column.GetProperty("type").GetString();
                        
                        _columnNames[i] = name;
                        _columnTypes[i] = type;
                        _columnMapping[name] = i;
                        
                        Console.WriteLine($"DEBUG: Column {i}: {name} ({type})");
                    }
                }
                else
                {
                    Console.WriteLine("DEBUG: No columns found in response");
                    
                    // Initialize empty columns if not found
                    if (_columnNames == null)
                    {
                        _columnNames = new string[0];
                        _columnTypes = new string[0];
                        _columnMapping = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                    }
                }

                // Extract data rows if available
                if (root.TryGetProperty("data", out JsonElement dataElement) && 
                    dataElement.ValueKind == JsonValueKind.Array)
                {
                    int rowCount = dataElement.GetArrayLength();
                    _hasRows = rowCount > 0;
                    _rowsEnumerator = dataElement.EnumerateArray();
                    _currentRow = -1;
                    
                    Console.WriteLine($"DEBUG: Found {rowCount} rows of data");
                }
                else
                {
                    Console.WriteLine("DEBUG: No data rows found in response");
                    _hasRows = false;
                }
                
                // Check if there's an error message
                if (root.TryGetProperty("error", out JsonElement errorElement))
                {
                    string errorType = errorElement.TryGetProperty("errorType", out JsonElement errorTypeElement) 
                        ? errorTypeElement.GetString() : "Unknown";
                    string errorMessage = errorElement.TryGetProperty("message", out JsonElement messageElement)
                        ? messageElement.GetString() : "Unknown error";
                        
                    Console.WriteLine($"DEBUG: Error found in response: {errorType} - {errorMessage}");
                    throw new Exception($"Trino error: {errorType} - {errorMessage}");
                }
                
                // Handle the case where Trino returns a response with stats but no data yet
                if (!_hasRows && string.IsNullOrEmpty(_nextUri) && 
                    !root.TryGetProperty("error", out _) && 
                    root.TryGetProperty("stats", out _))
                {
                    Console.WriteLine("DEBUG: Query completed successfully but returned no rows");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"DEBUG: Error processing response: {ex.Message}");
                throw;
            }
        }

        private void FetchNextBatch()
        {
            if (string.IsNullOrEmpty(_nextUri))
            {
                Console.WriteLine("DEBUG: FetchNextBatch called but nextUri is empty");
                return;
            }

            Console.WriteLine($"DEBUG: Fetching next batch from: {_nextUri}");
            int retryCount = 0;
            const int maxRetries = 3;
            TimeSpan retryDelay = TimeSpan.FromMilliseconds(100);

            while (retryCount < maxRetries)
            {
                try
                {
                    // Make a GET request to the next URI
                    var response = _httpClient.GetAsync(new Uri(_nextUri)).Result;
                    Console.WriteLine($"DEBUG: Got response with status: {response.StatusCode}");
                    
                    if (!response.IsSuccessStatusCode)
                    {
                        if ((int)response.StatusCode >= 500 && retryCount < maxRetries - 1)
                        {
                            // Server error, retry after a delay
                            retryCount++;
                            Console.WriteLine($"DEBUG: Server error {response.StatusCode}, retry {retryCount}/{maxRetries} after {retryDelay.TotalMilliseconds}ms");
                            Thread.Sleep(retryDelay);
                            retryDelay = TimeSpan.FromMilliseconds(retryDelay.TotalMilliseconds * 2); // Exponential backoff
                            continue;
                        }
                        
                        string responseContent = response.Content.ReadAsStringAsync().Result;
                        Console.WriteLine($"DEBUG: Error response content: {responseContent}");
                        throw new DbException($"Failed to fetch next batch: {response.StatusCode} - {responseContent}");
                    }

                    var responseContent = response.Content.ReadAsStringAsync().Result;
                    ProcessResponseData(responseContent);
                    return;
                }
                catch (Exception ex)
                {
                    if (ex is DbException)
                        throw;
                        
                    if (retryCount < maxRetries - 1)
                    {
                        retryCount++;
                        Console.WriteLine($"DEBUG: Error fetching batch, retry {retryCount}/{maxRetries}: {ex.Message}");
                        Thread.Sleep(retryDelay);
                        retryDelay = TimeSpan.FromMilliseconds(retryDelay.TotalMilliseconds * 2); // Exponential backoff
                    }
                    else
                    {
                        Console.WriteLine($"DEBUG: Max retries reached, throwing exception: {ex.Message}");
                        throw new DbException($"Error fetching next batch after {maxRetries} attempts: {ex.Message}", ex);
                    }
                }
            }
        }

        public override bool Read()
        {
            if (_closed)
                throw new InvalidOperationException("DataReader is closed");

            // If we don't have an enumerator yet, return false
            if (_rowsEnumerator.Equals(default(JsonElement.ArrayEnumerator)))
            {
                Console.WriteLine("DEBUG: Read called but no row enumerator is available");
                return false;
            }

            bool hasNext = false;
            try 
            {
                hasNext = _rowsEnumerator.MoveNext();
                Console.WriteLine($"DEBUG: Read attempt - has next row: {hasNext}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"DEBUG: Error moving to next row: {ex.Message}");
                return false;
            }
            
            if (hasNext)
            {
                _currentRow++;
                return true;
            }
            else if (!string.IsNullOrEmpty(_nextUri))
            {
                Console.WriteLine("DEBUG: No more rows in current batch, fetching next batch");
                // Fetch the next batch of data
                FetchNextBatch();
                return Read(); // Recursive call to attempt reading from the new batch
            }
            
            Console.WriteLine("DEBUG: No more rows and no next URI");
            return false;
        }

        public override bool IsDBNull(int ordinal)
        {
            if (_closed || _currentRow < 0)
                throw new InvalidOperationException("No current row");

            if (ordinal < 0 || ordinal >= FieldCount)
                throw new IndexOutOfRangeException($"Column index {ordinal} is out of range");

            var currentRow = _rowsEnumerator.Current;
            return currentRow[ordinal].ValueKind == JsonValueKind.Null;
        }

        public override object GetValue(int ordinal)
        {
            if (_closed || _currentRow < 0)
                throw new InvalidOperationException("No current row");

            if (ordinal < 0 || ordinal >= FieldCount)
                throw new IndexOutOfRangeException($"Column index {ordinal} is out of range");

            var currentRow = _rowsEnumerator.Current;
            var value = currentRow[ordinal];
            
            if (value.ValueKind == JsonValueKind.Null)
                return DBNull.Value;

            // Convert based on Trino type
            string trinoType = _columnTypes[ordinal].ToLowerInvariant();
            
            if (trinoType.Contains("varchar") || trinoType.Contains("char") || trinoType.Contains("text"))
                return value.GetString();
            else if (trinoType.Contains("int") || trinoType.Contains("tinyint") || trinoType.Contains("smallint"))
                return value.GetInt32();
            else if (trinoType.Contains("bigint"))
                return value.GetInt64();
            else if (trinoType.Contains("double") || trinoType.Contains("real"))
                return value.GetDouble();
            else if (trinoType.Contains("boolean"))
                return value.GetBoolean();
            else if (trinoType.Contains("date"))
                return DateTime.Parse(value.GetString());
            else if (trinoType.Contains("timestamp"))
                return DateTime.Parse(value.GetString());
            else if (trinoType.Contains("decimal"))
                return decimal.Parse(value.GetString());
            else
                return value.GetString(); // Default to string for unknown types
        }

        public override string GetName(int ordinal)
        {
            if (ordinal < 0 || ordinal >= FieldCount)
                throw new IndexOutOfRangeException($"Column index {ordinal} is out of range");

            return _columnNames[ordinal];
        }

        public override int GetOrdinal(string name)
        {
            if (_columnMapping.TryGetValue(name, out int ordinal))
                return ordinal;

            throw new IndexOutOfRangeException($"Column '{name}' not found");
        }

        public override bool GetBoolean(int ordinal)
        {
            object value = GetValue(ordinal);
            return Convert.ToBoolean(value);
        }

        public override byte GetByte(int ordinal)
        {
            object value = GetValue(ordinal);
            return Convert.ToByte(value);
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            throw new NotImplementedException("GetBytes is not implemented");
        }

        public override char GetChar(int ordinal)
        {
            object value = GetValue(ordinal);
            return Convert.ToChar(value);
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            throw new NotImplementedException("GetChars is not implemented");
        }

        public override DateTime GetDateTime(int ordinal)
        {
            object value = GetValue(ordinal);
            return Convert.ToDateTime(value);
        }

        public override decimal GetDecimal(int ordinal)
        {
            object value = GetValue(ordinal);
            return Convert.ToDecimal(value);
        }

        public override double GetDouble(int ordinal)
        {
            object value = GetValue(ordinal);
            return Convert.ToDouble(value);
        }

        public override Type GetFieldType(int ordinal)
        {
            if (ordinal < 0 || ordinal >= FieldCount)
                throw new IndexOutOfRangeException($"Column index {ordinal} is out of range");

            string trinoType = _columnTypes[ordinal].ToLowerInvariant();
            
            if (trinoType.Contains("varchar") || trinoType.Contains("char") || trinoType.Contains("text"))
                return typeof(string);
            else if (trinoType.Contains("int") || trinoType.Contains("tinyint") || trinoType.Contains("smallint"))
                return typeof(int);
            else if (trinoType.Contains("bigint"))
                return typeof(long);
            else if (trinoType.Contains("double"))
                return typeof(double);
            else if (trinoType.Contains("real"))
                return typeof(float);
            else if (trinoType.Contains("boolean"))
                return typeof(bool);
            else if (trinoType.Contains("date") || trinoType.Contains("timestamp"))
                return typeof(DateTime);
            else if (trinoType.Contains("decimal"))
                return typeof(decimal);
            else
                return typeof(string); // Default for unknown types
        }

        public override float GetFloat(int ordinal)
        {
            object value = GetValue(ordinal);
            return Convert.ToSingle(value);
        }

        public override Guid GetGuid(int ordinal)
        {
            string value = GetString(ordinal);
            return Guid.Parse(value);
        }

        public override short GetInt16(int ordinal)
        {
            object value = GetValue(ordinal);
            return Convert.ToInt16(value);
        }

        public override int GetInt32(int ordinal)
        {
            object value = GetValue(ordinal);
            return Convert.ToInt32(value);
        }

        public override long GetInt64(int ordinal)
        {
            object value = GetValue(ordinal);
            return Convert.ToInt64(value);
        }

        public override string GetString(int ordinal)
        {
            object value = GetValue(ordinal);
            return Convert.ToString(value);
        }

        public override int GetValues(object[] values)
        {
            if (values == null)
                throw new ArgumentNullException(nameof(values));

            int count = Math.Min(FieldCount, values.Length);
            for (int i = 0; i < count; i++)
            {
                values[i] = GetValue(i);
            }
            return count;
        }

        public override bool NextResult()
        {
            // Trino doesn't support multiple result sets in a single query
            return false;
        }

        public override int Depth => 0;

        public override bool IsClosed => _closed;

        public override int RecordsAffected => -1; // Not applicable for SELECT queries

        public override int FieldCount => _columnNames?.Length ?? 0;

        public override object this[int ordinal] => GetValue(ordinal);

        public override object this[string name] => GetValue(GetOrdinal(name));

        public override bool HasRows => _hasRows;

        public override void Close()
        {
            _closed = true;
            _currentResult?.Dispose();
        }

        public override DataTable GetSchemaTable()
        {
            if (_closed)
                throw new InvalidOperationException("DataReader is closed");

            var schemaTable = new DataTable("SchemaTable");
            
            // Add the standard schema columns
            schemaTable.Columns.Add("ColumnName", typeof(string));
            schemaTable.Columns.Add("ColumnOrdinal", typeof(int));
            schemaTable.Columns.Add("ColumnSize", typeof(int));
            schemaTable.Columns.Add("DataType", typeof(Type));
            schemaTable.Columns.Add("IsLong", typeof(bool));
            schemaTable.Columns.Add("IsReadOnly", typeof(bool));
            schemaTable.Columns.Add("IsUnique", typeof(bool));
            schemaTable.Columns.Add("IsKey", typeof(bool));
            schemaTable.Columns.Add("IsAutoIncrement", typeof(bool));
            schemaTable.Columns.Add("IsNullable", typeof(bool));
            
            // Populate the schema information for each column
            for (int i = 0; i < FieldCount; i++)
            {
                DataRow row = schemaTable.NewRow();
                row["ColumnName"] = _columnNames[i];
                row["ColumnOrdinal"] = i;
                row["ColumnSize"] = 0; // Unknown size
                row["DataType"] = GetFieldType(i);
                row["IsLong"] = false;
                row["IsReadOnly"] = true;
                row["IsUnique"] = false;
                row["IsKey"] = false;
                row["IsAutoIncrement"] = false;
                row["IsNullable"] = true; // Assume nullable by default
                
                schemaTable.Rows.Add(row);
            }
            
            return schemaTable;
        }
