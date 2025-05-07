using System.Data;
using System.Globalization; // Necesario para CultureInfo
using System.Text;
using MySqlConnector; // Asegúrate de tener instalado el paquete NuGet MySqlConnector

namespace MySQLDatabaseExporter
{
    class Program
    {
        // Ajusta el tamaño del lote según la memoria y el rendimiento deseado
        private const int FetchBatchSize = 5000; // Número de filas a leer de origen por lote
        private const int InsertBatchSize = 100; // Número de filas por sentencia INSERT INTO ... VALUES (...), (...);

        static async Task Main(string[] args)
        {
            Console.WriteLine("MySQL Database Exporter");
            Console.WriteLine("=======================");

            Dictionary<string, string> sourceConfig = null;
            string outputDirectory = null;
            bool isAutomated = args.Length > 0; // Determinar si se está ejecutando con argumentos

            try
            {
                if (isAutomated)
                {
                    // Modo automatizado: Parsear argumentos
                    Console.WriteLine("Detectados argumentos. Ejecutando en modo automatizado.");
                    sourceConfig = ParseArguments(args, out outputDirectory);

                    if (sourceConfig == null) // Falló el parseo o faltan argumentos requeridos
                    {
                        Console.WriteLine("\nError: Faltan argumentos requeridos o hay un error de sintaxis.");
                        PrintUsage();
                        return; // Salir si no se pudieron obtener los datos de conexión de los argumentos
                    }

                    // Asegurarse de que el directorio de salida no sea null o vacío (default ya hecho en ParseArguments)
                    if (string.IsNullOrWhiteSpace(outputDirectory))
                    {
                        outputDirectory = Directory.GetCurrentDirectory();
                    }
                }
                else
                {
                    // Modo interactivo: Pedir datos por consola
                    Console.WriteLine("No se detectaron argumentos. Ejecutando en modo interactivo.");
                    sourceConfig = GetConnectionConfig("ORIGEN");

                    Console.Write("\nIngrese el directorio de salida para el archivo SQL (deje vacío para el directorio actual): ");
                    outputDirectory = Console.ReadLine();
                    if (string.IsNullOrWhiteSpace(outputDirectory))
                    {
                        outputDirectory = Directory.GetCurrentDirectory();
                    }
                }

                // Validar la configuración de conexión obtenida (tanto si vino de args como de prompt)
                if (!sourceConfig.ContainsKey("Host") || string.IsNullOrWhiteSpace(sourceConfig["Host"]) ||
                    !sourceConfig.ContainsKey("User") || string.IsNullOrWhiteSpace(sourceConfig["User"]) ||
                    !sourceConfig.ContainsKey("Database") || string.IsNullOrWhiteSpace(sourceConfig["Database"]))
                {
                    Console.WriteLine("\nError: La información de conexión (Host, Usuario, Base de datos) es incompleta.");
                    if (isAutomated) PrintUsage();
                    return;
                }


                // Construir cadena de conexión
                string sourceConnectionString = BuildConnectionString(sourceConfig);
                string databaseName = sourceConfig["Database"]; // Obtenemos el nombre de la base de datos

                // Generar nombre de archivo de salida dinámicamente
                string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
                string safeDatabaseName = SanitizeFileName(databaseName); // Limpiar el nombre de la base de datos para usar en el archivo
                string generatedFileName = $"{safeDatabaseName}_{timestamp}.sql";
                string outputFile = Path.Combine(outputDirectory, generatedFileName);


                // Asegurarse de que el directorio de salida existe (puede ser necesario si se especificó por argumento)
                if (!Directory.Exists(outputDirectory))
                {
                    Directory.CreateDirectory(outputDirectory);
                    Console.WriteLine($"Directorio de salida creado: {outputDirectory}");
                }


                // 3. Obtener lista de tablas
                List<string> tables = await GetAllTables(sourceConnectionString);

                if (tables.Count == 0)
                {
                    Console.WriteLine("No se encontraron tablas para exportar en la base de datos de origen.");
                    if (!isAutomated) Console.ReadKey(); // Esperar solo en modo interactivo
                    return;
                }

                Console.WriteLine($"\nSe encontraron {tables.Count} tablas para exportar:");
                foreach (var table in tables)
                {
                    Console.WriteLine($" - {table}");
                }

                // En modo automatizado, no pedimos confirmación
                if (!isAutomated)
                {
                    Console.Write("¿Desea continuar con la exportación de estas tablas? (S/N): ");
                    string confirmExport = Console.ReadLine()?.ToUpper();

                    if (confirmExport != "S")
                    {
                        Console.WriteLine("Operación cancelada por el usuario.");
                        Console.ReadKey();
                        return;
                    }
                }


                // 4. Exportar esquema y datos a archivo
                Console.WriteLine($"\nIniciando exportación a: {outputFile}");

                // Usar FileStream y StreamWriter para escribir en el archivo
                using (var fs = new FileStream(outputFile, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 4096, useAsync: true))
                using (var writer = new StreamWriter(fs, Encoding.UTF8)) // Usar UTF8 para compatibilidad
                {
                    // Escribir encabezado típico de un dump SQL
                    await writer.WriteLineAsync("SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT;");
                    await writer.WriteLineAsync("SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS;");
                    await writer.WriteLineAsync("SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION;");
                    await writer.WriteLineAsync("/*!40101 SET NAMES utf8 */;");
                    await writer.WriteLineAsync("SET time_zone = '+00:00';"); // O el time_zone que necesites
                    await writer.WriteLineAsync("/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;");
                    await writer.WriteLineAsync("/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE */;");
                    await writer.WriteLineAsync("/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;");
                    await writer.WriteLineAsync("/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;");
                    await writer.WriteLineAsync("/*!40101 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;");
                    await writer.WriteLineAsync("--");
                    await writer.WriteLineAsync($"-- Exported from MySQL database '{databaseName}' using C# exporter on {DateTime.Now}");
                    await writer.WriteLineAsync("--");
                    await writer.WriteLineAsync(); // Línea en blanco


                    int tableCounter = 1;
                    foreach (var tableName in tables)
                    {
                        Console.WriteLine($"\n[{tableCounter}/{tables.Count}] Exportando tabla: {tableName}");

                        // Obtener y escribir DROP TABLE y CREATE TABLE
                        string createTableStatement = await GetCreateTableStatement(sourceConnectionString, tableName);
                        await writer.WriteLineAsync($"--");
                        await writer.WriteLineAsync($"-- Estructura para la tabla `{tableName}`");
                        await writer.WriteLineAsync($"--");
                        await writer.WriteLineAsync($"DROP TABLE IF EXISTS `{tableName}`;");
                        await writer.WriteLineAsync(createTableStatement + ";"); // Agregar punto y coma
                        await writer.WriteLineAsync(); // Línea en blanco

                        // Exportar datos
                        await writer.WriteLineAsync($"--");
                        await writer.WriteLineAsync($"-- Volcado de datos para la tabla `{tableName}`");
                        await writer.WriteLineAsync($"--");
                        await ExportTableData(sourceConnectionString, tableName, writer);
                        await writer.WriteLineAsync(); // Línea en blanco

                        tableCounter++;
                    }

                    // Escribir pie de página típico de un dump SQL
                    await writer.WriteLineAsync("/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;");
                    await writer.WriteLineAsync("/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;");
                    await writer.WriteLineAsync("/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;");
                    await writer.WriteLineAsync("/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;");
                    await writer.WriteLineAsync("/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;");
                    await writer.WriteLineAsync("/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;");
                    await writer.WriteLineAsync("/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;");
                    await writer.WriteLineAsync("-- Dump completed");
                }

                Console.WriteLine("\nExportación completada exitosamente.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\nError durante la exportación: {ex.Message}");
                // Console.WriteLine(ex.StackTrace); // Opcional: mostrar stack trace
                if (isAutomated) Environment.ExitCode = 1; // Indicar error al sistema operativo en modo automatizado
            }
            finally
            {
                // Esperar para que el usuario vea el resultado antes de cerrar (solo en modo interactivo)
                if (!isAutomated)
                {
                    Console.WriteLine("\nPresione cualquier tecla para salir.");
                    Console.ReadKey();
                }
            }
        }

        // Nuevo método para parsear argumentos de línea de comandos
        static Dictionary<string, string> ParseArguments(string[] args, out string outputDirectory)
        {
            var config = new Dictionary<string, string>();
            outputDirectory = null; // Inicializar como null

            // Mapeo de argumentos cortos/largos a claves del diccionario
            var argMap = new Dictionary<string, string>
             {
                 { "-h", "Host" },     { "--host", "Host" },
                 { "-P", "Port" },     { "--port", "Port" }, // MySQL uses -P for port
                 { "-u", "User" },     { "--user", "User" },
                 { "-p", "Password" }, { "--password", "Password" },
                 { "-d", "Database" }, { "--database", "Database" },
                 { "-o", "OutputDirectory" }, { "--output-dir", "OutputDirectory" }
             };

            for (int i = 0; i < args.Length; i++)
            {
                if (argMap.TryGetValue(args[i].ToLowerInvariant(), out string configKey))
                {
                    // Verificar si hay un valor siguiente
                    if (i + 1 < args.Length)
                    {
                        // El valor es el siguiente argumento
                        string value = args[i + 1];

                        // Si es el directorio de salida, lo manejamos por separado
                        if (configKey == "OutputDirectory")
                        {
                            outputDirectory = value;
                        }
                        else
                        {
                            config[configKey] = value;
                        }
                        i++; // Saltar el siguiente argumento ya que es el valor
                    }
                    else
                    {
                        // Argumento sin valor (ej: "-h" al final)
                        Console.WriteLine($"Error: El argumento '{args[i]}' requiere un valor.");
                        return null; // Indicar error de parseo
                    }
                }
                else
                {
                    // Argumento desconocido
                    Console.WriteLine($"Advertencia: Argumento desconocido '{args[i]}' será ignorado.");
                }
            }

            // Validar que los argumentos requeridos estén presentes
            if (!config.ContainsKey("Host") || string.IsNullOrWhiteSpace(config["Host"]) ||
                !config.ContainsKey("User") || string.IsNullOrWhiteSpace(config["User"]) ||
                !config.ContainsKey("Database") || string.IsNullOrWhiteSpace(config["Database"]))
            {
                // Faltan argumentos requeridos
                return null;
            }

            // Establecer puerto por defecto si no se especificó
            if (!config.ContainsKey("Port") || string.IsNullOrWhiteSpace(config["Port"]))
            {
                config["Port"] = "3306";
            }

            // Establecer directorio de salida por defecto si no se especificó por argumento
            if (string.IsNullOrWhiteSpace(outputDirectory))
            {
                outputDirectory = Directory.GetCurrentDirectory();
            }

            return config; // Devolver la configuración si el parseo fue exitoso
        }

        // Nuevo método para imprimir el uso de los argumentos de línea de comandos
        static void PrintUsage()
        {
            Console.WriteLine("\nUso:");
            Console.WriteLine("  MySqlDatabaseExporter.exe ");
            Console.WriteLine("  -h, --host      <host>         Host del servidor MySQL (requerido)");
            Console.WriteLine("  -P, --port      <puerto>       Puerto del servidor MySQL (por defecto 3306)");
            Console.WriteLine("  -u, --user      <usuario>      Usuario de la base de datos MySQL (requerido)");
            Console.WriteLine("  -p, --password  <contraseña>   Contraseña del usuario (requerido si no se usa autenticación sin contraseña)");
            Console.WriteLine("  -d, --database  <base_datos>   Nombre de la base de datos a exportar (requerido)");
            Console.WriteLine("  -o, --output-dir <directorio>  Directorio donde se guardará el archivo SQL (por defecto directorio actual)");
            Console.WriteLine("\nEjemplo:");
            Console.WriteLine("  MySqlDatabaseExporter.exe --host 192.168.1.100 -u admin -p secret -d mi_bd -o \"C:\\Backups\\MySQL\"");
        }


        static Dictionary<string, string> GetConnectionConfig(string serverLabel)
        {
            Console.WriteLine($"\nDetalles de conexión para el servidor {serverLabel}:");

            Console.Write("Host: ");
            string host = Console.ReadLine();

            Console.Write("Puerto (3306 por defecto): ");
            string port = Console.ReadLine();
            if (string.IsNullOrWhiteSpace(port))
                port = "3306";

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

            // Usar Console.KeyAvailable para leer sin bloquear
            while (true)
            {
                key = Console.ReadKey(true);

                if (key.Key == ConsoleKey.Enter)
                {
                    Console.WriteLine();
                    break;
                }
                else if (key.Key == ConsoleKey.Backspace && password.Length > 0)
                {
                    password = password.Substring(0, password.Length - 1);
                    Console.Write("\b \b");
                }
                else if (!char.IsControl(key.KeyChar))
                {
                    password += key.KeyChar;
                    Console.Write("*");
                }
            }

            return password;
        }

        static string BuildConnectionString(Dictionary<string, string> config)
        {
            config.TryGetValue("Host", out string host);
            config.TryGetValue("Port", out string port);
            config.TryGetValue("User", out string user);
            config.TryGetValue("Password", out string password);
            config.TryGetValue("Database", out string database);

            string server = string.IsNullOrWhiteSpace(host) ? "localhost" : host;
            string connectionPort = string.IsNullOrWhiteSpace(port) ? "3306" : port;
            string userId = string.IsNullOrWhiteSpace(user) ? "" : user;
            string dbPassword = password ?? ""; // Asegurarse de que la contraseña no sea null si no se proporcionó
            string dbName = string.IsNullOrWhiteSpace(database) ? "" : database;

            // Ya validamos los campos requeridos en Main o ParseArguments,
            // pero esta validación es útil si BuildConnectionString se usa en otro lugar.
            if (string.IsNullOrWhiteSpace(server) || string.IsNullOrWhiteSpace(userId) || string.IsNullOrWhiteSpace(dbName))
            {
                // Considerar lanzar una excepción aquí en lugar de solo advertir
                // throw new ArgumentException("Missing required connection parameters.");
                Console.WriteLine("Advertencia: Información de conexión incompleta."); // Mantener la advertencia por ahora
            }


            return $"Server={server};Port={connectionPort};User ID={userId};Password={dbPassword};Database={dbName};";
        }

        // Nuevo método para limpiar el nombre de archivo
        static string SanitizeFileName(string fileName)
        {
            // Reemplazar caracteres inválidos para nombres de archivo por un guion bajo
            foreach (char invalidChar in Path.GetInvalidFileNameChars())
            {
                fileName = fileName.Replace(invalidChar, '_');
            }
            // Opcional: Reemplazar espacios por guiones bajos
            fileName = fileName.Replace(' ', '_');
            return fileName;
        }


        static async Task<List<string>> GetAllTables(string connectionString)
        {
            Console.WriteLine("Obteniendo lista de tablas (excluyendo vistas) de la base de datos de origen...");

            List<string> tables = new List<string>();

            if (string.IsNullOrWhiteSpace(connectionString))
            {
                // Este caso ya debería manejarse antes de llamar aquí, pero es una buena defensa
                Console.WriteLine("Error: La cadena de conexión de origen está vacía.");
                return tables;
            }

            using (var connection = new MySqlConnection(connectionString))
            {
                try
                {
                    await connection.OpenAsync();
                    string query = "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = DATABASE()";
                    using (var command = new MySqlCommand(query, connection))
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            tables.Add(reader.GetString(0));
                        }
                    }
                }
                catch (MySqlException ex)
                {
                    Console.WriteLine($"Error de MySQL al obtener tablas: {ex.Message}");
                    throw;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error general al obtener tablas: {ex.Message}");
                    throw;
                }
            }
            return tables;
        }

        static async Task<string> GetCreateTableStatement(string connectionString, string tableName)
        {
            if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentNullException(nameof(connectionString));
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentNullException(nameof(tableName));

            using (var connection = new MySqlConnection(connectionString))
            {
                try
                {
                    await connection.OpenAsync();
                    using (var command = new MySqlCommand($"SHOW CREATE TABLE `{tableName}`", connection))
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
                catch (MySqlException ex)
                {
                    Console.WriteLine($"Error de MySQL al obtener estructura de '{tableName}': {ex.Message}");
                    throw;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error general al obtener estructura de '{tableName}': {ex.Message}");
                    throw;
                }
            }
        }

        static async Task<long> GetTableRowCount(string connectionString, string tableName)
        {
            if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentNullException(nameof(connectionString));
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentNullException(nameof(tableName));

            using (var connection = new MySqlConnection(connectionString))
            {
                try
                {
                    await connection.OpenAsync();
                    using (var command = new MySqlCommand($"SELECT COUNT(*) FROM `{tableName}`", connection))
                    {
                        var result = await command.ExecuteScalarAsync();
                        return result != null && result != DBNull.Value ? Convert.ToInt64(result) : 0;
                    }
                }
                catch (MySqlException ex)
                {
                    Console.WriteLine($"Error de MySQL al contar registros en '{tableName}': {ex.Message}");
                    throw;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error general al contar registros en '{tableName}': {ex.Message}");
                    throw;
                }
            }
        }

        static async Task<string> GetPrimaryKeyColumn(string connectionString, string tableName)
        {
            if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentNullException(nameof(connectionString));
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentNullException(nameof(tableName));

            using (var connection = new MySqlConnection(connectionString))
            {
                try
                {
                    await connection.OpenAsync();
                    string query = $"SHOW KEYS FROM `{tableName}` WHERE Key_name = 'PRIMARY'";
                    using (var command = new MySqlCommand(query, connection))
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        if (await reader.ReadAsync())
                        {
                            return reader.GetString("Column_name");
                        }
                        else
                        {
                            Console.WriteLine($"Advertencia: La tabla '{tableName}' no tiene una clave primaria. Se intentará ordenar por la primera columna.");
                            using (var getColumnsCommand = new MySqlCommand($"SHOW COLUMNS FROM `{tableName}`", connection))
                            using (var columnsReader = await getColumnsCommand.ExecuteReaderAsync())
                            {
                                if (await columnsReader.ReadAsync())
                                {
                                    return columnsReader.GetString("Field");
                                }
                            }
                            Console.WriteLine($"Advertencia: No se pudieron encontrar columnas para ordenar en la tabla '{tableName}'. La exportación de datos puede no ser fiable.");
                            return null;
                        }
                    }
                }
                catch (MySqlException ex)
                {
                    Console.WriteLine($"Error de MySQL al obtener clave primaria para '{tableName}': {ex.Message}");
                    throw;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error general al obtener clave primaria para '{tableName}': {ex.Message}");
                    throw;
                }
            }
        }


        // Modificamos el método EscapeSqlValue para que reciba el reader y el índice
        // para obtener el valor y su tipo correctamente.
        static string EscapeSqlValue(MySqlDataReader reader, int columnIndex)
        {
            // Si el valor es DBNull, devolvemos la palabra clave SQL "NULL"
            if (reader.IsDBNull(columnIndex))
            {
                return "NULL";
            }

            // Obtenemos el valor como objeto
            object value = reader.GetValue(columnIndex);

            // Ahora, formateamos según el tipo subyacente del valor
            switch (value)
            {
                case byte[] bytes:
                    // Exportar datos binarios (BLOB) como literal hexadecimal (0x...)
                    return $"0x{BitConverter.ToString(bytes).Replace("-", "")}";

                case bool boolean:
                    // MySQL representa booleanos como TinyInt(1)
                    return boolean ? "1" : "0";

                case DateTime dt:
                    // Formatear DateTime en un formato estándar de MySQL 'YYYY-MM-DD HH:mm:ss'
                    // Considerar el formato adecuado si incluye milisegundos (.fff)
                    return $"'{dt.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture)}'";

                case TimeSpan ts:
                    // Formato para TIME 'hh:mm:ss' o 'hh:mm:ss.ffffff' si incluye fracciones
                    // Usamos @"hh\:mm\:ss" y luego añadimos milisegundos si existen
                    string timeString = ts.ToString(@"hh\:mm\:ss", CultureInfo.InvariantCulture);
                    if (ts.Milliseconds > 0)
                    {
                        timeString += $".{ts.Milliseconds:D3}"; // Formato con 3 dígitos para milisegundos
                    }
                    return $"'{timeString}'";


                // Casos para tipos numéricos - Convertir a string usando CultureInfo.InvariantCulture
                // Esto asegura que los decimales usen '.' como separador, que es lo que espera SQL.
                // Usar "R" para floats/doubles para preservar la precisión de ida y vuelta
                case sbyte sb: return sb.ToString(CultureInfo.InvariantCulture);
                case byte b: return b.ToString(CultureInfo.InvariantCulture);
                case short s: return s.ToString(CultureInfo.InvariantCulture);
                case ushort us: return us.ToString(CultureInfo.InvariantCulture);
                case int i: return i.ToString(CultureInfo.InvariantCulture);
                case uint ui: return ui.ToString(CultureInfo.InvariantCulture);
                case long l: return l.ToString(CultureInfo.InvariantCulture);
                case ulong ul: return ul.ToString(CultureInfo.InvariantCulture);
                case float f: return f.ToString("R", CultureInfo.InvariantCulture);
                case double d: return d.ToString("R", CultureInfo.InvariantCulture);
                case decimal m: return m.ToString(CultureInfo.InvariantCulture);

                case string str:
                    // Para strings, usamos un escape manual básico que reemplaza ' por '' y \ por \\
                    // Esto es menos robusto que usar un método de escape de la biblioteca que considere la codificación y sql_mode,
                    // pero funciona en muchos casos y no requiere un objeto Command/Connection.
                    return $"'{str.Replace("'", "''").Replace("\\", "\\\\")}'";


                default:
                    // Fallback: Si no es un tipo conocido manejado arriba, intentar obtenerlo como string
                    // y aplicar el escape manual básico.
                    Console.WriteLine($"Advertencia: Tipo de dato '{value.GetType().Name}' desconocido para exportar en columna {reader.GetName(columnIndex)}. Intentando convertir a string y escapar.");
                    string stringValue = value.ToString(); // Obtener la representación string
                    return $"'{stringValue.Replace("'", "''").Replace("\\", "\\\\")}'"; // Escape básico

            }
        }


        // Nuevo método para exportar datos de una tabla
        static async Task ExportTableData(string connectionString, string tableName, StreamWriter writer)
        {
            if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentNullException(nameof(connectionString));
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentNullException(nameof(tableName));
            if (writer == null) throw new ArgumentNullException(nameof(writer));


            long totalRecords = await GetTableRowCount(connectionString, tableName);
            if (totalRecords == 0)
            {
                Console.WriteLine("No hay datos para exportar.");
                return;
            }

            string orderByColumn = await GetPrimaryKeyColumn(connectionString, tableName);
            if (string.IsNullOrWhiteSpace(orderByColumn))
            {
                Console.WriteLine($"Saltando exportación de datos para '{tableName}' debido a la falta de una columna de ordenación fiable.");
                return;
            }

            string selectQuery = $"SELECT * FROM `{tableName}` ORDER BY `{orderByColumn}` LIMIT @limit OFFSET @offset";


            long exportedRecords = 0;
            int offset = 0;
            List<string> columns = new List<string>(); // Para almacenar los nombres de las columnas

            // Obtener nombres de columnas una vez
            try
            {
                using (var conn = new MySqlConnection(connectionString))
                {
                    await conn.OpenAsync();
                    using (var cmd = new MySqlCommand($"SELECT * FROM `{tableName}` LIMIT 0", conn)) // Obtener esquema sin datos
                    using (var reader = await cmd.ExecuteReaderAsync(CommandBehavior.SchemaOnly))
                    {
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            columns.Add($"`{reader.GetName(i)}`");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al obtener nombres de columnas para '{tableName}': {ex.Message}");
                throw; // Re-lanzar
            }

            string columnsList = string.Join(", ", columns);


            while (exportedRecords < totalRecords)
            {
                // Es mejor usar una sola conexión abierta por lote para el reader
                using (var connection = new MySqlConnection(connectionString))
                {
                    try
                    {
                        await connection.OpenAsync();
                        using (var command = new MySqlCommand(selectQuery, connection))
                        {
                            command.Parameters.AddWithValue("@limit", FetchBatchSize);
                            command.Parameters.AddWithValue("@offset", offset);

                            // Usar CommandBehavior.SinglePass para leer de forma eficiente si es aplicable
                            using (var reader = await command.ExecuteReaderAsync()) // Eliminamos SinglePass ya que puede no ser universalmente soportado o útil en todos los drivers
                            {
                                int rowsInInsertBatch = 0; // Contador para las filas DENTRO de la sentencia INSERT actual
                                StringBuilder insertBatch = new StringBuilder();
                                int rowsReadThisBatch = 0; // Contador real de filas leídas en esta iteración de FetchBatchSize

                                while (await reader.ReadAsync())
                                {
                                    // Iniciar una nueva sentencia INSERT si es el comienzo de un lote de inserción
                                    if (rowsInInsertBatch % InsertBatchSize == 0)
                                    {
                                        if (rowsInInsertBatch > 0)
                                        {
                                            insertBatch.AppendLine(";"); // Terminar el INSERT anterior si no es el primero del bucle
                                        }
                                        insertBatch.Append($"INSERT INTO `{tableName}` ({columnsList}) VALUES ");
                                    }
                                    else
                                    {
                                        insertBatch.Append(", "); // Separador para multi-row INSERT
                                    }

                                    // Escribir los valores para la fila actual
                                    insertBatch.Append("(");
                                    for (int i = 0; i < reader.FieldCount; i++)
                                    {
                                        if (i > 0) insertBatch.Append(", ");

                                        // *** Llamada corregida a EscapeSqlValue ***
                                        // Ahora pasamos el reader y el índice de la columna
                                        insertBatch.Append(EscapeSqlValue(reader, i));
                                    }
                                    insertBatch.Append(")");

                                    rowsInInsertBatch++;
                                    rowsReadThisBatch++; // Incrementa el contador de filas leídas en este lote (hasta FetchBatchSize)
                                }

                                // Escribir cualquier INSERT parcial que haya quedado al final del reader
                                if (rowsInInsertBatch > 0)
                                {
                                    insertBatch.AppendLine(";");
                                    await writer.WriteAsync(insertBatch.ToString());
                                }

                                exportedRecords += rowsReadThisBatch; // Sumar las filas REALMENTE leídas
                                offset += FetchBatchSize; // Mover al siguiente bloque basado en el tamaño del lote solicitado

                                // Reportar progreso con una frecuencia razonable
                                // Reportar cada X filas o si es el último lote
                                bool isLastBatch = rowsReadThisBatch < FetchBatchSize || exportedRecords >= totalRecords;
                                if (exportedRecords == 0 || exportedRecords % (totalRecords / 100 + 1) == 0 || isLastBatch)
                                {
                                    long currentProgress = Math.Min(exportedRecords, totalRecords);
                                    double percentage = (double)currentProgress / totalRecords * 100;
                                    Console.Write($"\rExportando datos de '{tableName}': {currentProgress}/{totalRecords} registros ({percentage:F2}%)");
                                }


                                // Si el número de filas leídas en este lote es menor que el tamaño del lote solicitado,
                                // significa que hemos llegado al final de los datos de la tabla.
                                if (rowsReadThisBatch < FetchBatchSize)
                                {
                                    break; // Salir del bucle while (exportedRecords < totalRecords)
                                }

                            } // using reader
                        } // using command
                    } // try (inner)
                    catch (MySqlException ex)
                    {
                        Console.WriteLine($"\nError de MySQL al exportar datos de '{tableName}' (Offset de lectura: {offset}): {ex.Message}");
                        throw; // Re-lanzar
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"\nError general al exportar datos de '{tableName}' (Offset de lectura: {offset}): {ex.Message}");
                        throw; // Re-lanzar
                    }
                } // using connection
            } // while (exportedRecords < totalRecords)

            // Asegurar que se muestra 100% al final si hubo datos que exportar
            if (totalRecords > 0)
            {
                Console.Write($"\rExportando datos de '{tableName}': {totalRecords}/{totalRecords} registros (100.00%)\n");
            }
        }


    } // Fin de la clase Program
} // Fin del namespace