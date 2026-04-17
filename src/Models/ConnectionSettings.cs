using System.Data.Common;
using System.Data.SqlClient;

namespace XperienceCommunity.DatabaseAnonymizer.Models
{
    /// <summary>
    /// Represents the settings required to connect to the Kentico database.
    /// </summary>
    internal class ConnectionSettings
    {
        /// <summary>
        /// An optional full connection string. When set, it takes precedence over the individual properties,
        /// except that any non-empty individual property will override the corresponding value in the string.
        /// </summary>
        public string? ConnectionString { get; set; }


        /// <summary>
        /// The data source.
        /// </summary>
        public string? DataSource { get; set; }


        /// <summary>
        /// The user ID.
        /// </summary>
        public string? UserID { get; set; }


        /// <summary>
        /// The password.
        /// </summary>
        public string? Password { get; set; }


        /// <summary>
        /// The database name.
        /// </summary>
        public string? DatabaseName { get; set; }



        /// <summary>
        /// Converts the model properties into a SQL connection string.
        /// </summary>
        public string ToConnectionString()
        {
            // Parse with the lenient base builder so keywords unknown to System.Data.SqlClient
            // (e.g. "Command Timeout") do not throw during parsing...
            var parsed = new DbConnectionStringBuilder();
            if (!string.IsNullOrWhiteSpace(ConnectionString))
            {
                parsed.ConnectionString = ConnectionString;
            }

            if (!string.IsNullOrEmpty(DataSource))
            {
                parsed["Data Source"] = DataSource;
            }

            if (!string.IsNullOrEmpty(UserID))
            {
                parsed["User ID"] = UserID;
            }

            if (!string.IsNullOrEmpty(Password))
            {
                parsed["Password"] = Password;
            }

            if (!string.IsNullOrEmpty(DatabaseName))
            {
                parsed["Initial Catalog"] = DatabaseName;
            }

            // ...then copy only keywords supported by System.Data.SqlClient, since Kentico uses
            // that provider internally and will throw on unknown keywords when opening the connection.
            var sqlBuilder = new SqlConnectionStringBuilder();
            foreach (string key in parsed.Keys)
            {
                try
                {
                    sqlBuilder[key] = parsed[key];
                }
                catch (ArgumentException)
                {
                    // Silently drop keywords the SqlClient provider does not recognize
                    // (e.g. "Command Timeout" is only supported by Microsoft.Data.SqlClient).
                }
            }

            return sqlBuilder.ConnectionString;
        }


        /// <summary>
        /// Returns the database name parsed from <see cref="ConnectionString"/>, if any.
        /// </summary>
        public string? GetDatabaseFromConnectionString()
        {
            if (string.IsNullOrWhiteSpace(ConnectionString))
            {
                return null;
            }

            var builder = new DbConnectionStringBuilder { ConnectionString = ConnectionString };
            if (builder.TryGetValue("Initial Catalog", out var value)
                || builder.TryGetValue("Database", out value))
            {
                string name = value.ToString() ?? string.Empty;

                return string.IsNullOrEmpty(name) ? null : name;
            }

            return null;
        }
    }
}
