using System.Data;
using System.Text.Json;
using MySqlConnector;

namespace MySQLDatabaseCopier
{
    class Program
    {
        private static readonly string _configFilePath = Path.Combine(
            AppContext.BaseDirectory, // O AppDomain.CurrentDomain.BaseDirectory
            "configurations.json");

        private const int FetchBatchSize = 5000; // Tamaño del lote para la lectura desde origen

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

                // Obtener solo las tablas de la base de datos origen
                List<string> tables = await GetAllTables(sourceConnectionString);

                if (tables.Count == 0)
                {
                    Console.WriteLine("No se encontraron tablas para copiar en la base de datos de origen.");
                    return;
                }

                Console.WriteLine($"Se encontraron {tables.Count} tablas para copiar en la base de datos de origen:");
                foreach (var table in tables)
                {
                    Console.WriteLine($" - {table}");
                }


                int tableCounter = 1;
                foreach (var tableName in tables)
                {
                    Console.WriteLine($"\n[{tableCounter}/{tables.Count}] Procesando tabla: {tableName}");
                    await CopyTableData(sourceConnectionString, targetConnectionString, tableName);
                    tableCounter++;
                }

                Console.WriteLine("\nProceso completado exitosamente.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
            }

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

        // --- START: Modificaciones para carga seccionada ---

        static async Task CopyTableData(string sourceConnectionString, string targetConnectionString, string tableName)
        {
            try
            {
                Console.WriteLine($"Obteniendo estructura de la tabla '{tableName}'...");
                string createTableStatement = await GetCreateTableStatement(sourceConnectionString, tableName);

                Console.WriteLine($"Obteniendo clave primaria para '{tableName}'...");
                string primaryKeyColumn = await GetPrimaryKeyColumn(sourceConnectionString, tableName);

                // Obtener el número total de registros
                long totalRecords = await GetTableRowCount(sourceConnectionString, tableName);
                Console.WriteLine($"Tabla '{tableName}': {totalRecords} registros encontrados.");

                if (totalRecords == 0)
                {
                    Console.WriteLine("No hay datos para copiar.");
                    // Aunque no haya datos, recreamos la tabla en destino
                    await DropAndCreateTable(targetConnectionString, tableName, createTableStatement);
                    Console.WriteLine($"Tabla '{tableName}' creada vacía en destino.");
                    return;
                }

                // Aplicar cambios en el servidor destino (crear/recrear la tabla)
                await DropAndCreateTable(targetConnectionString, tableName, createTableStatement);

                long processedRecords = 0;
                int offset = 0;
                int progressInterval = totalRecords > 10000 ? 1000 : (totalRecords > 1000 ? 100 : 10); // Intervalo de progreso dinámico

                Console.WriteLine($"Copiando datos de la tabla '{tableName}' en lotes de {FetchBatchSize}...");

                while (processedRecords < totalRecords)
                {
                    // Obtener un lote de datos del origen
                    DataTable batchData = await GetTableDataBatch(sourceConnectionString, tableName, FetchBatchSize, offset, primaryKeyColumn);

                    if (batchData.Rows.Count == 0)
                    {
                        // Esto debería ocurrir solo si el totalRecords inicial fue incorrecto o hubo modificaciones concurrentes
                        Console.WriteLine("\nNo se encontraron más registros en el lote actual. Finalizando copia de tabla.");
                        break;
                    }

                    // Copiar el lote de datos al destino
                    await CopyDataToTargetTable(targetConnectionString, tableName, batchData);

                    processedRecords += batchData.Rows.Count;
                    offset += batchData.Rows.Count; // Incrementar el offset por el número real de filas procesadas en este lote

                    // Mostrar progreso
                    if (processedRecords % progressInterval == 0 || processedRecords == totalRecords)
                    {
                        double percentage = (double)processedRecords / totalRecords * 100;
                        Console.Write($"\rProgreso tabla '{tableName}': {processedRecords}/{totalRecords} registros ({percentage:F2}%)");
                    }
                }
                Console.Write($"\rProgreso tabla '{tableName}': {processedRecords}/{totalRecords} registros (100.00%)\n"); // Asegurarse de mostrar 100% al final


            }
            catch (Exception ex)
            {
                Console.WriteLine($"\nError al copiar la tabla {tableName}: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
                // Decide si quieres lanzar la excepción para detener el programa o continuar con la siguiente tabla
                // throw; // Descomenta esto si un error en una tabla debe detener todo el proceso
            }
        }

        static async Task<long> GetTableRowCount(string connectionString, string tableName)
        {
            using (var connection = new MySqlConnection(connectionString))
            {
                await connection.OpenAsync();
                using (var command = new MySqlCommand($"SELECT COUNT(*) FROM `{tableName}`", connection))
                {
                    var result = await command.ExecuteScalarAsync();
                    return result != DBNull.Value ? Convert.ToInt64(result) : 0;
                }
            }
        }

        static async Task<string> GetPrimaryKeyColumn(string connectionString, string tableName)
        {
            using (var connection = new MySqlConnection(connectionString))
            {
                await connection.OpenAsync();
                // Consulta para encontrar la columna PRIMARY KEY. Considera solo la primera columna si es compuesta.
                string query = $"SHOW KEYS FROM `{tableName}` WHERE Key_name = 'PRIMARY'";
                using (var command = new MySqlCommand(query, connection))
                {
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        if (await reader.ReadAsync())
                        {
                            // La columna 'Column_name' contiene el nombre de la columna en el índice.
                            return reader.GetString("Column_name");
                        }
                        else
                        {
                            // Si no hay clave primaria, intentamos ordenar por la primera columna si existe
                            Console.WriteLine($"Advertencia: La tabla '{tableName}' no tiene una clave primaria. Se intentará ordenar por la primera columna. La copia puede no ser fiable si la tabla no tiene un orden natural consistente.");
                            using (var getColumnsCommand = new MySqlCommand($"SHOW COLUMNS FROM `{tableName}`", connection))
                            {
                                using (var columnsReader = await getColumnsCommand.ExecuteReaderAsync())
                                {
                                    if (await columnsReader.ReadAsync())
                                    {
                                        return columnsReader.GetString("Field"); // Devuelve el nombre de la primera columna
                                    }
                                }
                            }
                            throw new Exception($"No se pudo encontrar una clave primaria ni columnas para ordenar en la tabla '{tableName}'.");
                        }
                    }
                }
            }
        }

        static async Task<DataTable> GetTableDataBatch(string connectionString, string tableName, int limit, int offset, string orderByColumn)
        {
            // Asegurar que orderByColumn esté entre comillas inversas si no lo está ya
            if (!orderByColumn.StartsWith("`")) orderByColumn = $"`{orderByColumn}`";
            if (!orderByColumn.EndsWith("`")) orderByColumn = $"{orderByColumn}`";


            Console.Write($"\rObteniendo lote (Offset: {offset}, Limit: {limit}) de '{tableName}'..."); // Actualizar el mensaje de obtención

            DataTable dataTable = new DataTable();

            using (var connection = new MySqlConnection(connectionString))
            {
                await connection.OpenAsync();
                // Consulta para obtener un lote específico ordenado por la clave primaria
                string query = $"SELECT * FROM `{tableName}` ORDER BY {orderByColumn} LIMIT {limit} OFFSET {offset}";

                using (var command = new MySqlCommand(query, connection))
                {
                    // No es necesario añadir parámetros para LIMIT/OFFSET, ya que son parte de la sintaxis de la consulta
                    // MySQL Connector/NET maneja esto directamente en el comando.

                    using (var adapter = new MySqlDataAdapter(command))
                    {
                        adapter.Fill(dataTable);
                    }
                }
            }

            return dataTable;
        }

        // Este método ahora recibe un DataTable que contiene un LOTE de datos
        static async Task CopyDataToTargetTable(string connectionString, string tableName, DataTable dataTable)
        {
            // La verificación de dataTable.Rows.Count == 0 se hace ahora en CopyTableData antes de llamar aquí
            // Console.WriteLine($"Copiando {dataTable.Rows.Count} registros a la tabla destino..."); // Ya se muestra progreso en CopyTableData

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

                // Usamos INSERT IGNORE INTO para manejar posibles duplicados si la tabla tiene UNIQUE/PRIMARY KEY
                // Otra opción es usar REPLACE INTO si quieres reemplazar en caso de duplicado
                string insertQuery = $"INSERT IGNORE INTO `{tableName}` ({string.Join(", ", columns)}) VALUES ({string.Join(", ", parameters)})";

                // Batch insert para mejorar el rendimiento DENTRO DE ESTE LOTE
                int insertBatchSize = 100; // Tamaño del lote para la inserción en destino
                int totalRecordsInBatch = dataTable.Rows.Count;

                MySqlTransaction transaction = null;

                try
                {
                    transaction = await connection.BeginTransactionAsync();

                    for (int i = 0; i < totalRecordsInBatch; i++)
                    {
                        using (var command = new MySqlCommand(insertQuery, connection, transaction))
                        {
                            command.Parameters.Clear(); // Limpiar parámetros del comando anterior
                            foreach (DataColumn column in dataTable.Columns)
                            {
                                command.Parameters.AddWithValue($"@{column.ColumnName}",
                                    dataTable.Rows[i][column.ColumnName] ?? DBNull.Value);
                            }

                            await command.ExecuteNonQueryAsync();
                        }

                        // Commit cada insertBatchSize registros dentro del lote actual
                        if ((i + 1) % insertBatchSize == 0 || (i + 1) == totalRecordsInBatch)
                        {
                            await transaction.CommitAsync();
                            if ((i + 1) < totalRecordsInBatch)
                            {
                                transaction = await connection.BeginTransactionAsync(); // Iniciar nueva transacción para el siguiente lote de inserción
                            }
                        }
                    }

                    // Console.WriteLine($"Lote de {totalRecordsInBatch} registros copiado."); // Mensaje de lote copiado
                }
                catch (Exception ex)
                {
                    if (transaction != null)
                    {
                        await transaction.RollbackAsync();
                    }
                    throw new Exception($"Error al insertar datos en el lote actual de la tabla '{tableName}': {ex.Message}", ex);
                }
            }
        }

        static async Task<string> GetCreateTableStatement(string connectionString, string tableName)
        {
            // El código para obtener la estructura de la tabla es el mismo
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


        static async Task DropAndCreateTable(string connectionString, string tableName, string createTableStatement)
        {
            // El código para eliminar y crear la tabla es el mismo
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


    }

    // Clase para serializar/deserializar las configuraciones
    public class ConnectionPair
    {
        public Dictionary<string, string> Source { get; set; } = new Dictionary<string, string>();
        public Dictionary<string, string> Target { get; set; } = new Dictionary<string, string>();
    }
}