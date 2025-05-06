using System.Data;
using MySqlConnector;

namespace MySQLDatabaseCopier
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("MySQL Database Copier");
            Console.WriteLine("=====================");

            try
            {
                // Obtener configuración de conexiones
                var sourceConfig = GetConnectionConfig("ORIGEN");
                var targetConfig = GetConnectionConfig("DESTINO");

                // Construir cadenas de conexión
                string sourceConnectionString = BuildConnectionString(sourceConfig);
                string targetConnectionString = BuildConnectionString(targetConfig);

                // Preguntar si se desea copiar todas las tablas o una específica

                // Obtener todas las tablas de la base de datos origen
                List<string> tables = await GetAllTables(sourceConnectionString);

                if (tables.Count == 0)
                {
                    Console.WriteLine("No se encontraron tablas en la base de datos de origen.");
                    return;
                }

                Console.WriteLine($"Se encontraron {tables.Count} tablas en la base de datos de origen.");
                foreach (var table in tables)
                {
                    Console.WriteLine($" - {table}");
                }

                int tableCount = 1;
                foreach (var tableName in tables)
                {
                    Console.WriteLine($"\n[{tableCount}/{tables.Count}] Procesando tabla: {tableName}");
                    await CopyTableData(sourceConnectionString, targetConnectionString, tableName);
                    tableCount++;
                }

                Console.WriteLine("\nProceso completado exitosamente.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
            }

            Console.WriteLine("Presione cualquier tecla para salir...");
            Console.ReadKey();
        }

        static Dictionary<string, string> GetConnectionConfig(string serverLabel)
        {
            Console.WriteLine($"\nConfiguración del servidor {serverLabel}:");

            Console.Write("Host: ");
            string host = Console.ReadLine();

            Console.Write("Puerto: ");
            string port = Console.ReadLine();
            if (string.IsNullOrWhiteSpace(port))
                port = "3306"; // Puerto por defecto de MySQL

            Console.Write("Usuario: ");
            string user = Console.ReadLine();

            Console.Write("Contraseña: ");
            string password = GetPasswordMasked();

            Console.Write("Base de datos: ");
            string database = Console.ReadLine();

            return new Dictionary<string, string>
            {
                { "Host", host },
                { "Port", port },
                { "User", user },
                { "Password", password },
                { "Database", database }
            };
        }

        static string GetPasswordMasked()
        {
            string password = "";
            ConsoleKeyInfo key;

            do
            {
                key = Console.ReadKey(true);

                if (key.Key != ConsoleKey.Enter && key.Key != ConsoleKey.Backspace)
                {
                    password += key.KeyChar;
                    Console.Write("*");
                }
                else if (key.Key == ConsoleKey.Backspace && password.Length > 0)
                {
                    password = password.Substring(0, password.Length - 1);
                    Console.Write("\b \b");
                }
            } while (key.Key != ConsoleKey.Enter);

            Console.WriteLine();
            return password;
        }

        static string BuildConnectionString(Dictionary<string, string> config)
        {
            return $"Server={config["Host"]};Port={config["Port"]};User ID={config["User"]};Password={config["Password"]};Database={config["Database"]};";
        }

        static async Task<List<string>> GetAllTables(string connectionString)
        {
            Console.WriteLine("Obteniendo lista de tablas de la base de datos origen...");

            List<string> tables = new List<string>();

            using (var connection = new MySqlConnection(connectionString))
            {
                await connection.OpenAsync();

                using (var command = new MySqlCommand("SHOW TABLES", connection))
                {
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            tables.Add(reader.GetString(0));
                        }
                    }
                }
            }

            return tables;
        }

        static async Task CopyTableData(string sourceConnectionString, string targetConnectionString, string tableName)
        {
            try
            {
                // Obtener la estructura de la tabla y datos
                string createTableStatement = await GetCreateTableStatement(sourceConnectionString, tableName);
                DataTable tableData = await GetTableData(sourceConnectionString, tableName);

                // Si estamos copiando múltiples tablas, no pedimos confirmación para cada una
                bool isMultiTableOperation = false;

                // Verificamos si hay una tabla anterior en el stack de llamadas
                System.Diagnostics.StackTrace stackTrace = new System.Diagnostics.StackTrace();
                for (int i = 1; i < stackTrace.FrameCount; i++)
                {
                    var method = stackTrace.GetFrame(i).GetMethod();
                    if (method.Name == "Main" && method.DeclaringType.Name == "Program")
                    {
                        isMultiTableOperation = true;
                        break;
                    }
                }
                Console.WriteLine($"Tabla: {tableName} - {tableData.Rows.Count} registros encontrados");
                // Aplicar cambios en el servidor destino
                await DropAndCreateTable(targetConnectionString, tableName, createTableStatement);
                await CopyDataToTargetTable(targetConnectionString, tableName, tableData);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al copiar la tabla {tableName}: {ex.Message}");
                // Si estamos en modo multi-tabla, continuamos con la siguiente
                if (ex.Message.Contains("doesn't exist"))
                {
                    Console.WriteLine($"La tabla {tableName} no existe en la base de datos origen. Continuando con la siguiente tabla.");
                    return;
                }
            }
        }

        static async Task<string> GetCreateTableStatement(string connectionString, string tableName)
        {
            Console.WriteLine($"Obteniendo estructura de la tabla '{tableName}'...");

            using (var connection = new MySqlConnection(connectionString))
            {
                await connection.OpenAsync();

                using (var command = new MySqlCommand($"SHOW CREATE TABLE `{tableName}`", connection))
                {
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        if (await reader.ReadAsync())
                        {
                            return reader.GetString(1);
                        }
                        else
                        {
                            throw new Exception($"No se pudo obtener la estructura de la tabla {tableName}");
                        }
                    }
                }
            }
        }

        static async Task<DataTable> GetTableData(string connectionString, string tableName)
        {
            Console.WriteLine($"Obteniendo datos de la tabla '{tableName}'...");

            DataTable dataTable = new DataTable();

            using (var connection = new MySqlConnection(connectionString))
            {
                await connection.OpenAsync();

                using (var command = new MySqlCommand($"SELECT * FROM `{tableName}`", connection))
                {
                    using (var adapter = new MySqlDataAdapter(command))
                    {
                        adapter.Fill(dataTable);
                    }
                }
            }

            return dataTable;
        }

        static async Task DropAndCreateTable(string connectionString, string tableName, string createTableStatement)
        {
            Console.WriteLine($"Eliminando y creando tabla '{tableName}' en el servidor destino...");

            using (var connection = new MySqlConnection(connectionString))
            {
                await connection.OpenAsync();

                // Drop table if exists
                using (var dropCommand = new MySqlCommand($"DROP TABLE IF EXISTS `{tableName}`", connection))
                {
                    await dropCommand.ExecuteNonQueryAsync();
                }

                // Create table
                using (var createCommand = new MySqlCommand(createTableStatement, connection))
                {
                    await createCommand.ExecuteNonQueryAsync();
                }
            }
        }

        static async Task CopyDataToTargetTable(string connectionString, string tableName, DataTable dataTable)
        {
            if (dataTable.Rows.Count == 0)
            {
                Console.WriteLine("No hay datos para copiar.");
                return;
            }

            Console.WriteLine($"Copiando {dataTable.Rows.Count} registros a la tabla destino...");

            using (var connection = new MySqlConnection(connectionString))
            {
                await connection.OpenAsync();

                // Construir la consulta de inserción
                var columns = new List<string>();
                var parameters = new List<string>();

                foreach (DataColumn column in dataTable.Columns)
                {
                    columns.Add($"`{column.ColumnName}`");
                    parameters.Add($"@{column.ColumnName}");
                }

                string insertQuery = $"INSERT INTO `{tableName}` ({string.Join(", ", columns)}) VALUES ({string.Join(", ", parameters)})";

                // Batch insert para mejorar el rendimiento
                int batchSize = 100;
                int totalRecords = dataTable.Rows.Count;
                int processedRecords = 0;

                // Verificar si la tabla tiene muchos registros para ajustar el intervalo de progreso
                int progressInterval = totalRecords > 1000 ? 100 : 10;

                // No usamos using para la transacción, la administramos manualmente
                MySqlTransaction transaction = await connection.BeginTransactionAsync();

                try
                {
                    for (int i = 0; i < totalRecords; i++)
                    {
                        using (var command = new MySqlCommand(insertQuery, connection, transaction))
                        {
                            foreach (DataColumn column in dataTable.Columns)
                            {
                                command.Parameters.AddWithValue($"@{column.ColumnName}",
                                    dataTable.Rows[i][column.ColumnName] ?? DBNull.Value);
                            }

                            await command.ExecuteNonQueryAsync();
                        }

                        processedRecords++;

                        // Mostrar progreso
                        if (processedRecords % progressInterval == 0 || processedRecords == totalRecords)
                        {
                            double percentage = (double)processedRecords / totalRecords * 100;
                            Console.Write($"\rProgreso: {processedRecords}/{totalRecords} registros ({percentage:F2}%)");
                        }

                        // Commit cada batchSize registros
                        if (processedRecords % batchSize == 0 || processedRecords == totalRecords)
                        {
                            await transaction.CommitAsync();

                            // Iniciar una nueva transacción si no hemos terminado
                            if (processedRecords < totalRecords)
                            {
                                transaction = await connection.BeginTransactionAsync();
                            }
                        }
                    }

                    Console.WriteLine("\nRegistros copiados exitosamente.");
                }
                catch (Exception ex)
                {
                    if (transaction != null)
                    {
                        await transaction.RollbackAsync();
                    }
                    throw new Exception($"Error al insertar datos: {ex.Message}", ex);
                }
            }
        }
    }
}
