using System.Data;
using System.Text.Json;
using MySqlConnector;

namespace MySQLDatabaseCopier
{
    class Program
    {
        private static readonly string _configFilePath = Path.Combine( AppContext.BaseDirectory, "configurations.json");

        static async Task Main(string[] args)
        {
            Console.WriteLine("MySQL Database Copier");
            Console.WriteLine("=====================");

            try
            {
                // Crear directorio de configuración si no existe
                string configDir = Path.GetDirectoryName(_configFilePath);
                if (!Directory.Exists(configDir))
                {
                    Directory.CreateDirectory(configDir);
                }

                // Verificar si se especificó un nombre de configuración por línea de comandos
                string configName = null;
                if (args.Length > 0)
                {
                    configName = args[0];
                }

                // Cargar configuraciones existentes o mostrar menú de selección
                Dictionary<string, ConnectionPair> savedConfigs = LoadSavedConfigurations();
                ConnectionPair selectedConfig = null;

                if (configName != null)
                {
                    // Usar la configuración especificada por línea de comandos
                    if (savedConfigs.TryGetValue(configName, out selectedConfig))
                    {
                        Console.WriteLine($"Usando configuración: {configName}");
                    }
                    else
                    {
                        Console.WriteLine($"No se encontró la configuración '{configName}'");
                        return;
                    }
                }
                else
                {
                    // Mostrar menú para seleccionar, crear o administrar configuraciones
                    selectedConfig = ManageConfigurations(savedConfigs);
                    if (selectedConfig == null)
                    {
                        return; // Usuario canceló la operación
                    }
                }

                // Construir cadenas de conexión
                string sourceConnectionString = BuildConnectionString(selectedConfig.Source);
                string targetConnectionString = BuildConnectionString(selectedConfig.Target);

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

                Console.Write("¿Desea continuar con la copia de todas las tablas? (S/N): ");
                string confirmCopy = Console.ReadLine().ToUpper();

                if (confirmCopy != "S")
                {
                    Console.WriteLine("Operación cancelada por el usuario.");
                    return;
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

        static ConnectionPair ManageConfigurations(Dictionary<string, ConnectionPair> savedConfigs)
        {
            while (true)
            {
                Console.Clear();
                Console.WriteLine("Administración de Configuraciones");
                Console.WriteLine("===============================");

                if (savedConfigs.Count > 0)
                {
                    Console.WriteLine("\nConfiguraciones guardadas:");
                    int index = 1;
                    foreach (var config in savedConfigs)
                    {
                        Console.WriteLine($"{index}. {config.Key}");
                        Console.WriteLine($"   Origen: {config.Value.Source["Host"]}, BD: {config.Value.Source["Database"]}");
                        Console.WriteLine($"   Destino: {config.Value.Target["Host"]}, BD: {config.Value.Target["Database"]}");
                        index++;
                    }
                }
                else
                {
                    Console.WriteLine("\nNo hay configuraciones guardadas.");
                }

                Console.WriteLine("\nOpciones:");
                Console.WriteLine("1. Usar una configuración existente");
                Console.WriteLine("2. Crear nueva configuración");
                Console.WriteLine("3. Eliminar configuración existente");
                Console.WriteLine("4. Salir");

                Console.Write("\nSeleccione una opción: ");
                string option = Console.ReadLine();

                switch (option)
                {
                    case "1":
                        if (savedConfigs.Count == 0)
                        {
                            Console.WriteLine("No hay configuraciones para seleccionar. Cree una nueva primero.");
                            Console.WriteLine("Presione cualquier tecla para continuar...");
                            Console.ReadKey();
                            continue;
                        }

                        Console.Write("Ingrese el número de la configuración a usar: ");
                        if (int.TryParse(Console.ReadLine(), out int configIndex) &&
                            configIndex >= 1 && configIndex <= savedConfigs.Count)
                        {
                            string key = savedConfigs.Keys.ElementAt(configIndex - 1);
                            return savedConfigs[key];
                        }
                        else
                        {
                            Console.WriteLine("Selección inválida.");
                            Console.WriteLine("Presione cualquier tecla para continuar...");
                            Console.ReadKey();
                        }
                        break;

                    case "2":
                        ConnectionPair newConfig = CreateNewConfiguration();
                        Console.Write("Ingrese un nombre para esta configuración: ");
                        string configName = Console.ReadLine();

                        if (string.IsNullOrWhiteSpace(configName))
                        {
                            Console.WriteLine("El nombre no puede estar vacío.");
                        }
                        else if (savedConfigs.ContainsKey(configName))
                        {
                            Console.Write("Ya existe una configuración con ese nombre. ¿Desea sobrescribirla? (S/N): ");
                            if (Console.ReadLine().ToUpper() == "S")
                            {
                                savedConfigs[configName] = newConfig;
                                SaveConfigurations(savedConfigs);
                                Console.WriteLine("Configuración guardada exitosamente.");

                                Console.Write("¿Desea usar esta configuración ahora? (S/N): ");
                                if (Console.ReadLine().ToUpper() == "S")
                                {
                                    return newConfig;
                                }
                            }
                        }
                        else
                        {
                            savedConfigs[configName] = newConfig;
                            SaveConfigurations(savedConfigs);
                            Console.WriteLine("Configuración guardada exitosamente.");

                            Console.Write("¿Desea usar esta configuración ahora? (S/N): ");
                            if (Console.ReadLine().ToUpper() == "S")
                            {
                                return newConfig;
                            }
                        }

                        Console.WriteLine("Presione cualquier tecla para continuar...");
                        Console.ReadKey();
                        break;

                    case "3":
                        if (savedConfigs.Count == 0)
                        {
                            Console.WriteLine("No hay configuraciones para eliminar.");
                            Console.WriteLine("Presione cualquier tecla para continuar...");
                            Console.ReadKey();
                            continue;
                        }

                        Console.Write("Ingrese el número de la configuración a eliminar: ");
                        if (int.TryParse(Console.ReadLine(), out int deleteIndex) &&
                            deleteIndex >= 1 && deleteIndex <= savedConfigs.Count)
                        {
                            string keyToDelete = savedConfigs.Keys.ElementAt(deleteIndex - 1);

                            Console.Write($"¿Está seguro de que desea eliminar la configuración '{keyToDelete}'? (S/N): ");
                            if (Console.ReadLine().ToUpper() == "S")
                            {
                                savedConfigs.Remove(keyToDelete);
                                SaveConfigurations(savedConfigs);
                                Console.WriteLine("Configuración eliminada exitosamente.");
                            }
                        }
                        else
                        {
                            Console.WriteLine("Selección inválida.");
                        }

                        Console.WriteLine("Presione cualquier tecla para continuar...");
                        Console.ReadKey();
                        break;

                    case "4":
                        return null;

                    default:
                        Console.WriteLine("Opción inválida.");
                        Console.WriteLine("Presione cualquier tecla para continuar...");
                        Console.ReadKey();
                        break;
                }
            }
        }

        static ConnectionPair CreateNewConfiguration()
        {
            Console.WriteLine("\nCreando nueva configuración");
            Console.WriteLine("==========================");

            // Configuración del servidor origen
            var sourceConfig = GetConnectionConfig("ORIGEN");

            // Configuración del servidor destino
            var targetConfig = GetConnectionConfig("DESTINO");

            return new ConnectionPair
            {
                Source = sourceConfig,
                Target = targetConfig
            };
        }

        static Dictionary<string, ConnectionPair> LoadSavedConfigurations()
        {
            if (!File.Exists(_configFilePath))
            {
                return new Dictionary<string, ConnectionPair>();
            }

            try
            {
                string json = File.ReadAllText(_configFilePath);
                return JsonSerializer.Deserialize<Dictionary<string, ConnectionPair>>(json) ??
                       new Dictionary<string, ConnectionPair>();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al cargar configuraciones: {ex.Message}");
                return new Dictionary<string, ConnectionPair>();
            }
        }

        static void SaveConfigurations(Dictionary<string, ConnectionPair> configs)
        {
            try
            {
                string json = JsonSerializer.Serialize(configs, new JsonSerializerOptions
                {
                    WriteIndented = true
                });
                File.WriteAllText(_configFilePath, json);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al guardar configuraciones: {ex.Message}");
            }
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
            Console.WriteLine("Obteniendo lista de tablas (excluyendo vistas) de la base de datos origen...");

            List<string> tables = new List<string>();

            using (var connection = new MySqlConnection(connectionString))
            {
                await connection.OpenAsync();

                // Consulta para obtener solo los nombres de las tablas (TABLE_TYPE = 'BASE TABLE')
                // del esquema actual (TABLE_SCHEMA = DATABASE())
                string query = "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = DATABASE()";

                using (var command = new MySqlCommand(query, connection))
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

    // Clase para serializar/deserializar las configuraciones
    public class ConnectionPair
    {
        public Dictionary<string, string> Source { get; set; } = new Dictionary<string, string>();
        public Dictionary<string, string> Target { get; set; } = new Dictionary<string, string>();
    }
}