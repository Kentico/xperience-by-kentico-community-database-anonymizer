using Microsoft.Data.SqlClient;

namespace XperienceCommunity.DatabaseAnonymizer.Models
{
    /// <summary>
    /// Represents the settings required to connect to the Kentico database.
    /// </summary>
    internal class ConnectionSettings
    {
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
            var builder = new SqlConnectionStringBuilder() { TrustServerCertificate = true };
            if (!string.IsNullOrEmpty(DataSource))
            {
                builder.DataSource = DataSource;
            }

            if (!string.IsNullOrEmpty(UserID))
            {
                builder.UserID = UserID;
            }

            if (!string.IsNullOrEmpty(Password))
            {
                builder.Password = Password;
            }

            if (!string.IsNullOrEmpty(DatabaseName))
            {
                builder.InitialCatalog = DatabaseName;
            }

            return builder.ConnectionString;
        }
    }
}
