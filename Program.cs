using Microsoft.Data.SqlClient;
using Npgsql;
using Newtonsoft.Json;
using System.Text.RegularExpressions;
using System.Data;
using System.ComponentModel;
using System.Text;
using System;

var schema_migration =  new schema_migration();
var data_migration = new data_migration();
schema_migration.start();
data_migration.start();
public static class connections
{
    public static string mssql_data_source = "127.0.0.1";
    public static string mssql_database = "master";
    public static string mssql_user_id = "abood";
    public static string mssql_user_password = "123";
    public static string psql_data_source = "127.0.0.1";
    public static string psql_database = "testing";
    public static string psql_user_id = "postgres";
    public static string psql_user_password = "123";
    public static int max_rows_at_once = 100;
}
public static class helpers {
    public static string database_tables_query = String.Empty;
    public static string mssql_connection_string = $"Data Source={connections.mssql_data_source};database={"{0}"};User id={connections.mssql_user_id};Password={connections.mssql_user_password};TrustServerCertificate=true";
    public static string psql_connection_string = $"Host={connections.psql_data_source};Username={connections.psql_user_id};Password={connections.psql_user_password};Database={"{0}"}";
    private static string reserved_keyWords = "{\"all\": 1, \"analyse\": 1, \"analyze\": 1, \"and\": 1, \"any\": 1, \"array\": 1, \"as\": 1, \"asc\": 1, \"asymmetric\": 1, \"both\": 1, \"case\": 1, \"cast\": 1, \"check\": 1, \"collate\": 1, \"column\": 1, \"constraint\": 1, \"create\": 1, \"current_date\": 1, \"current_role\": 1, \"current_time\": 1, \"current_timestamp\": 1, \"current_user\": 1, \"default\": 1, \"deferrable\": 1, \"desc\": 1, \"distinct\": 1, \"do\": 1, \"else\": 1, \"end\": 1, \"except\": 1, \"false\": 1, \"for\": 1, \"foreign\": 1, \"from\": 1, \"grant\": 1, \"group\": 1, \"having\": 1, \"in\": 1, \"initially\": 1, \"intersect\": 1, \"into\": 1, \"leading\": 1, \"limit\": 1, \"localtime\": 1, \"localtimestamp\": 1, \"new\": 1, \"not\": 1, \"null\": 1, \"off\": 1, \"offset\": 1, \"old\": 1, \"on\": 1, \"only\": 1, \"or\": 1, \"order\": 1, \"placing\": 1, \"primary\": 1, \"references\": 1, \"returning\": 1, \"select\": 1, \"session_user\": 1, \"some\": 1, \"symmetric\": 1, \"table\": 1, \"then\": 1, \"to\": 1, \"trailing\": 1, \"true\": 1, \"union\": 1, \"unique\": 1, \"user\": 1, \"using\": 1, \"when\": 1, \"where\": 1, \"with\": 1}";
    public static NpgsqlConnection conn = new NpgsqlConnection(String.Format(helpers.psql_connection_string,connections.psql_database));
    public static dynamic? reserved_keyWords_dictionary = JsonConvert.DeserializeObject(reserved_keyWords);
    public static void get_databases_name(List<string>databases,string connection_string)
    {
        SqlConnection connection = new SqlConnection(connection_string);
        try
        {
            connection.Open();
            string command = "Select name from sys.databases";
            SqlCommand query = new SqlCommand(command, connection);
            SqlDataReader reader = query.ExecuteReader(CommandBehavior.SequentialAccess);
            while (reader.Read())
            {
                string name = reader.GetString(0);
                if (name.ToLower().StartsWith("work"))
                    databases.Add(name.ToLower());
            }
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            Environment.Exit(-1);
        }
        connection.Close();
    }
    public static void fetch_tables(string databaseName, string mmsql_string, string postgres_string,Action<string,string,string,string>object_function,bool table_migration)
    {
        List<string> tables_name = new();
        string query = "SELECT name FROM sys.tables";
        mmsql_string = String.Format(mmsql_string, databaseName);
        postgres_string = string.Format(postgres_string, databaseName);
        using (SqlConnection connect = new SqlConnection(mmsql_string))
        {
            SqlCommand query_result = new SqlCommand(query, connect);
            connect.Open();
            SqlDataReader result = query_result.ExecuteReader(CommandBehavior.SequentialAccess);
            helpers.database_tables_query = "";
            while (result.Read())
            {
                string table_name = result.GetString(0);
                object_function.Invoke(table_name, mmsql_string, postgres_string,databaseName);
                if (table_migration == false)
                {
                    Console.WriteLine("Done with " + table_name);
                }
            }
            if (table_migration == true)
            {
                using (NpgsqlConnection transaaction_connection = new NpgsqlConnection(postgres_string))
                {
                    transaaction_connection.Open();
                    NpgsqlTransaction transction = transaaction_connection.BeginTransaction();
                    NpgsqlCommand command = new NpgsqlCommand(helpers.database_tables_query, transaaction_connection, transction);
                    NpgsqlCommand migration_log = new NpgsqlCommand($"update logs_migrate_schema set current_state = 2 where databasename='{databaseName}'", helpers.conn);
                    try
                    {
                        command.ExecuteNonQuery();
                        transction.Commit();
                        migration_log.ExecuteNonQuery();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(helpers.database_tables_query);
                        transction.Rollback();
                        Console.WriteLine(e);
                        Environment.Exit(-1);
                    }
                }
            }
        }
    }
}
public class schema_migration {

    private string connection_string;
    private string connection_string_postgres;
    List<string> databases;
    public schema_migration()
    {
        this.connection_string = String.Format(helpers.mssql_connection_string, connections.mssql_database);
        this.connection_string_postgres = String.Format(helpers.psql_connection_string, connections.psql_database);
        this.databases = new List<string>();
        /*
           schema migration
            database name
            varchar 5000
            state int as follows:
            1-created database but no schema 
            2-created database with shcema
            --------------------------------------
           data migration
            offset_r last position of last good readings //i need to update this to something else
        */
        string logs_table = """
            create table IF NOT EXISTS logs_migrate_schema
            (
            	databasename varchar(5000) null,
            	current_state int null
            );
            """;
        using (NpgsqlCommand command = new NpgsqlCommand(logs_table, helpers.conn))
        {
            helpers.conn.Open();
            try
            {
                command.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                //failed! create log file and store the e.message there!
                Environment.Exit(-1);

            }
        }
    }
    public void start()
    {
        helpers.get_databases_name(databases,connection_string);
        this.create_databases();
        this.create_tables();
    }
    public void create_databases()
    {
        for (int i = 0; i < this.databases.Count; i++)
        {
            var create_database = $"create DATABASE {this.databases[i]} with ENCODING = 'UTF8'";
            using (NpgsqlConnection current_seassion = new NpgsqlConnection(connection_string_postgres))
            {
                current_seassion.Open();
                NpgsqlCommand create_database_command = new NpgsqlCommand(create_database, current_seassion);
                try
                {
                    NpgsqlCommand logs_migrate_schema = new NpgsqlCommand($"select current_state from logs_migrate_schema where databasename='{this.databases[i]}'", helpers.conn);
                    int? x = (int?)logs_migrate_schema.ExecuteScalar();
                    if (x == 1 || x == 2)
                        continue;
                    else if (x != 1)
                    {
                        create_database_command.ExecuteNonQuery();
                        logs_migrate_schema.CommandText = $"Insert into logs_migrate_schema(databasename,current_state) values('{this.databases[i]}',1)";
                        logs_migrate_schema.ExecuteNonQuery();
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e + "its heere");
                    Environment.Exit(-1);
                }
            }
        }
    }
    public void create_tables()
    {
        for(int i = 0; i < this.databases.Count;i++)
        {
            helpers.fetch_tables(this.databases[i], helpers.mssql_connection_string, helpers.psql_connection_string, table,true);
        }
    }
    public void table(string table_name, string connection,string postgres_string,string databasename)
    {
        helpers.database_tables_query += $"create table IF NOT EXISTS {table_name}(\n";
        using (SqlConnection connect = new(connection))
        {
            var ok = false;
            connect.Open();
            var query = $"select COLUMN_NAME,  COLUMN_DEFAULT,IS_NULLABLE,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,CHARACTER_OCTET_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,DATETIME_PRECISION from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME=  '{table_name}' order by ORDINAL_POSITION";
            var constrains = $"SELECT COLUMN_NAME,c.CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS c join  INFORMATION_SCHEMA.KEY_COLUMN_USAGE c1 on c.CONSTRAINT_NAME = c1.CONSTRAINT_NAME where c1.TABLE_NAME = '{table_name}' and c.CONSTRAINT_TYPE ='PRIMARY KEY'";
            SqlCommand command = new(query, connect);
            SqlDataReader read = command.ExecuteReader(CommandBehavior.SequentialAccess);
            while (read.Read())
            {
                string? COLUMN_NAME = null;
                string? COLUMN_DEFAULT = null;
                string? IS_NULLABLE = null;
                string? DATA_TYPE = null;
                long? CHARACTER_MAXIMUM_LENGTH = null;
                long? CHARACTER_OCTET_LENGTH = null;
                long? NUMERIC_PRECISION = null;
                long? NUMERIC_SCALE = null;
                long? datetime_precision = null;
                if (read.IsDBNull(0) == false)
                    COLUMN_NAME = read.GetString(0);
                if (read.IsDBNull(1) == false)
                    COLUMN_DEFAULT = read.GetString(1);
                if (read.IsDBNull(2) == false)
                    IS_NULLABLE = read.GetString(2);
                if (read.IsDBNull(3) == false)
                    DATA_TYPE = read.GetString(3);
                if (read.IsDBNull(4) == false)
                    CHARACTER_MAXIMUM_LENGTH = read.GetInt32(4);
                if (read.IsDBNull(5) == false)
                    CHARACTER_OCTET_LENGTH = read.GetInt32(5);
                if (read.IsDBNull(6) == false)
                    NUMERIC_PRECISION = (long)read.GetByte(6);
                if (read.IsDBNull(7) == false)
                    NUMERIC_SCALE = (long)read.GetInt32(7);
                if (read.IsDBNull(8) == false)
                    datetime_precision = (long)read.GetInt16(8);
                if (COLUMN_NAME != null)
                {
                    if (helpers.reserved_keyWords_dictionary != null && helpers.reserved_keyWords_dictionary[COLUMN_NAME.ToLower()] != null)
                    {
                        helpers.database_tables_query += $"\"{COLUMN_NAME.ToLower()}\" ";
                    }
                    else
                        helpers.database_tables_query += COLUMN_NAME + " ";
                }
                if (DATA_TYPE != null)
                {
                    string new_Type = String.Empty;
                    DATA_TYPE = DATA_TYPE.ToLower();
                    if (DATA_TYPE.Contains("char") || DATA_TYPE.Contains("text"))
                    {
                        if (CHARACTER_MAXIMUM_LENGTH == null || DATA_TYPE.Contains("text") || CHARACTER_MAXIMUM_LENGTH > 10485760 || CHARACTER_MAXIMUM_LENGTH == -1)
                        {
                            new_Type = "text"; //choose text if no length was specified!
                        }
                        else
                        {
                            new_Type = $"varchar({CHARACTER_MAXIMUM_LENGTH})";
                        }
                    }
                    else if (DATA_TYPE == "tinyint")
                    {
                        new_Type = "smallint";
                    }
                    else if (DATA_TYPE == "decimal" || DATA_TYPE == "numeric")
                    {
                        new_Type = $"{DATA_TYPE}({NUMERIC_PRECISION},{NUMERIC_SCALE})";

                    }
                    else if (DATA_TYPE == "smallmoney")
                    {
                        new_Type = "money"; // check more
                    }
                    else if (DATA_TYPE == "float" || DATA_TYPE == "real")
                    {
                        new_Type = "double precision";
                    }
                    else if (DATA_TYPE == "time")
                    {
                        if (datetime_precision >= 7)
                            new_Type = "time(6)";
                        else
                            new_Type = $"time({datetime_precision})";
                    }
                    else if (DATA_TYPE.Contains("binary"))
                    {
                        new_Type = "bytea";
                    }
                    else if (DATA_TYPE == "uniqueidentifier")
                    {
                        new_Type = "uuid";
                    }
                    else if (DATA_TYPE == "smalldatetime")
                    {
                        new_Type = "timestamp(0)";
                    }
                    else if (DATA_TYPE == "datetime")
                    {
                        new_Type = "timestamp(3)";
                    }
                    else if (DATA_TYPE == "datetime2")
                    {
                        if (datetime_precision >= 7)
                            new_Type = "timestamp(6)";
                        else
                            new_Type = $"timestamp({datetime_precision})";
                    }
                    else if (DATA_TYPE == "bit")
                    {
                        new_Type = "boolean";
                    }
                    else if (DATA_TYPE == "datetimeoffset")
                    {
                        if (datetime_precision >= 7)
                            new_Type = "timestamp(6) with time zone";
                        else
                            new_Type = $"timestampz({datetime_precision}) with time zone";
                    }
                    else if (DATA_TYPE == "image")
                    {
                        new_Type = "bytea";
                    }
                    else if (DATA_TYPE == "sql_variant")
                    {
                        new_Type = "varchar(10485759)";
                    }
                    else if (DATA_TYPE == "timestamp")
                    {
                        new_Type = "bytea";
                    }
                    else
                        new_Type += DATA_TYPE;
                    helpers.database_tables_query += new_Type + ' ';
                }
                if (IS_NULLABLE != null)
                {
                    string Null = "null ";
                    if (IS_NULLABLE.ToLower() == "no")
                    {
                        Null = "not null";
                    }
                    helpers.database_tables_query += Null + ",\n";
                }
                // here you can put the default
            }
            using (SqlConnection connect_pk = new(connection))
            {
                connect_pk.Open();
                SqlCommand pl = new(constrains, connect_pk);
                string? COLUMN_NAME_ = null;
                string? CONSTRAINT_TYPE = null;
                string? CONSTRAINT_NAME = null;
                SqlDataReader read_me = pl.ExecuteReader(CommandBehavior.SequentialAccess);
                List<string> pk = new();
                while (read_me.Read())
                {
                    if (read_me.IsDBNull(0) == false)
                        COLUMN_NAME_ = read_me.GetString(0);
                    if (read_me.IsDBNull(1) == false)
                        CONSTRAINT_NAME = read_me.GetString(1);
                    if (COLUMN_NAME_ != null)
                        pk.Add(COLUMN_NAME_);
                }
                if (pk.Count > 0)
                {
                    ok = true;
                    helpers.database_tables_query += $"CONSTRAINT {CONSTRAINT_NAME} PRIMARY KEY(";
                    for (int i = 0; i < pk.Count - 1; i++)
                        helpers.database_tables_query += $"{pk[i]},";
                    helpers.database_tables_query += $"{pk[pk.Count - 1]}";
                }
            }
            if (!ok)
                for (int i = helpers.database_tables_query.Length - 2; i >= 0; i--)
                {
                    if (helpers.database_tables_query[i] == ',')
                    {
                        helpers.database_tables_query = helpers.database_tables_query.Substring(0, i);
                        break;
                    }
                    else
                    {
                        break;
                    }
                }
            else
                helpers.database_tables_query += ')';
            helpers.database_tables_query += ");\n";
        }
        }
    }
public class data_migration
{
    private string connection_string;
    private string connection_string_postgres;
    private List<string> databases;
    public data_migration()
    {
        this.connection_string = String.Format(helpers.mssql_connection_string,connections.mssql_database);
        this.connection_string_postgres = String.Format(helpers.psql_connection_string, connections.psql_database);
        this.databases = new();
     //  helpers.conn.Open();
        string logs_table = """
            create table IF NOT EXISTS logs_migrate_data
            (
            	databasename varchar(5000) null,
                tablename varchar(5000) null,
            	offset_last int null
            );
            """;
        using (NpgsqlCommand command = new NpgsqlCommand(logs_table, helpers.conn))
        {
            try
            {
                command.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                //failed! create log file and store the e.message there!
                Environment.Exit(-1);

            }
        }
    }
    public void start()
    {
        helpers.get_databases_name(databases,connection_string);
        this.databases2();
    }
    public void databases2()
    {
        for (int i = 0; i < this.databases.Count; i++)
        {
            helpers.fetch_tables(this.databases[i], helpers.mssql_connection_string, helpers.psql_connection_string, copy_data,false);
        }
    }
    public void copy_data(string table_name,string mmsql_string, string postgres_string,string databasename)
    {
        var query = "SELECT * FROM {0} order by 1 offset {1} rows fetch next {2} rows only;";
        SqlCommand commander = new(query, new SqlConnection(mmsql_string));
        commander.Connection.Open();
        NpgsqlCommand migration_log = new NpgsqlCommand($"select offset_last from logs_migrate_data where databasename='{databasename}' and tablename = '{table_name}'", helpers.conn);
        System.Int32? x = (System.Int32?)migration_log.ExecuteScalar();
        var readed = (x == null) ? 0 : x;
        var done = false;
        do
        {
            commander.CommandText = string.Format(query,  table_name, readed,connections.max_rows_at_once);
            SqlDataReader read_tables = commander.ExecuteReader();
            var c = 0;
            while (read_tables.Read())
            {
                c++;
                helpers.database_tables_query+=  $"insert into {table_name} (";
                for (int i = 0; i < read_tables.FieldCount; i++)
                {
                    string name = read_tables.GetName(i);
                    if (read_tables.FieldCount == i + 1)
                    {
                        if (helpers.reserved_keyWords_dictionary[name] != null)
                        {
                            helpers.database_tables_query += $"\"{name}\"" + ") ";
                        }
                        else
                            helpers.database_tables_query += name + ") ";
                    }
                    else
                    if (helpers.reserved_keyWords_dictionary[name] != null)
                    {
                        helpers.database_tables_query += $"\"{name}\"" + ", ";
                    }
                    else
                        helpers.database_tables_query += name + ", ";
                }
                helpers.database_tables_query += "values (";
                for (int i = 0; i < read_tables.FieldCount; i++)
                {
                    dynamic type_var = "null";
                    Type type = read_tables.GetFieldType(i);
                    if (read_tables.IsDBNull(i) == false)
                    try
                    {
                        if (type == typeof(System.Int64))
                        {
                            type_var = (Int64)read_tables.GetInt64(i);
                        }
                        else if (type == typeof(System.Byte[]))
                        {
                            long start_index = 0;          
                            long size = read_tables.GetBytes(i, 0, null, 0, 0);
                            byte[] buffer = new byte[size];
                            read_tables.GetBytes(i, 0, buffer, 0, buffer.Length);
                            string hex = "0x";
                            hex += string.Join("", buffer.Select(x => x.ToString("X2")));
                            type_var = $"'{hex}'";
                        }
                        else if (type == typeof(System.String))
                        {
                            type_var = read_tables.GetString(i); // getchars can also work depend on use case
                            type_var = type_var.Replace("'", "''");
                            type_var = $"'{type_var}'";
                        }
                        else if (type == typeof(System.DateTime))
                        {
                            type_var = read_tables.GetDateTime(i);
                            type_var = $"'{type_var}'";
                        }
                        else if (type == typeof(System.DateTimeOffset))
                        {
                            type_var = read_tables.GetDateTimeOffset(i);
                            type_var = $"'{type_var}'";
                        }
                        else if (type == typeof(System.Decimal))
                        {
                            type_var = read_tables.GetDecimal(i);
                        }
                        else if (type == typeof(System.Double))
                        {
                            type_var = read_tables.GetDouble(i);
                        }
                        else if (type == typeof(System.Int32))
                        {
                            type_var = read_tables.GetInt32(i);
                        }
                        else if (type == typeof(System.Int16))
                        {
                            type_var = read_tables.GetInt16(i);
                        }
                        else if (type == typeof(System.TimeSpan))
                        {
                            type_var = read_tables.GetTimeSpan(i);
                            type_var = $"'{type_var}'";
                        }
                        else if (type == typeof(System.Byte))
                        {
                            type_var = read_tables.GetByte(i);
                        }
                        else if (type == typeof(System.Guid))
                        {
                            type_var = read_tables.GetGuid(i);
                            type_var = $"'{type_var}'";
                        }
                        else {
                            {
                                type_var = read_tables.GetValue(i)?.ToString();
                                if (((string)type_var).Trim().Length == 0)
                                {
                                    type_var = "null";
                                }
                            }
                        }
                    }
                    catch (Exception e) { type_var = "null"; }

                    if (read_tables.FieldCount == i + 1)
                        helpers.database_tables_query += $"{type_var});";
                    else
                        helpers.database_tables_query += $"{type_var},";
                }
                helpers.database_tables_query += '\n';
            }
            if (c == 0)
                break;
            if (c < connections.max_rows_at_once)
                done = true;
                readed += c;
                using (NpgsqlConnection transaaction_connection = new NpgsqlConnection(postgres_string))
                {
                    transaaction_connection.Open();
                    NpgsqlTransaction transction = transaaction_connection.BeginTransaction();
                    NpgsqlCommand command = new NpgsqlCommand(helpers.database_tables_query, transaaction_connection, transction);
                    migration_log.CommandText  = ($"select offset_last from logs_migrate_data where databasename='{transaaction_connection.Database}' and tablename = '{table_name}'");
                    x = (System.Int32?)migration_log.ExecuteScalar();
                    try
                    {
                        if (x == null)
                        {
                            command.ExecuteNonQuery();
                            transction.Commit();
                            migration_log.CommandText = $"insert into logs_migrate_data (databasename,tablename,offset_last) values('{transaaction_connection.Database}','{table_name}',{readed})";
                            migration_log.ExecuteNonQuery();
                        }
                        else
                        {
                            command.ExecuteNonQuery();
                            transction.Commit();
                            migration_log.CommandText = $"update logs_migrate_data set offset_last = {readed} where databasename='{transaaction_connection.Database}' and tablename = '{table_name}'";
                            migration_log.ExecuteNonQuery();
                        }
                    }
                    catch (Exception e)
                    {
                        transction.Rollback();
                        Console.WriteLine(helpers.database_tables_query);
                        Console.WriteLine(e + "its hereeeee");
                        Environment.Exit(-1);
                    }
                    finally {
                        helpers.database_tables_query = "";
                    }
                read_tables.Close();
            }
        } while (!done);
    }
}