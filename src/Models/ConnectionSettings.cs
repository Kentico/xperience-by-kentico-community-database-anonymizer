using System.Data;
using System.Data.Common;
using System.Data.SqlClient;

using CMS.DataEngine;

using CMS.Helpers;

using Spectre.Console;

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
        /// If <c>true</c>, Windows authentication is used.
        /// </summary>
        public bool IntegratedSecurity { get; set; }


        /// <summary>
        /// Displays a prompt to select a database from the provided data source.
        /// </summary>
        public void SetDatabaseFromPrompt()
        {
            var databaseNames = GetDatabaseNames();
            if (!databaseNames.Any())
            {
                throw new InvalidOperationException("Failed to retrieve databases from server");
            }

            string databaseTitle = $"[{Constants.PROMPT_COLOR}]Database:[/] ";
            DatabaseName = AnsiConsole.Prompt(new SelectionPrompt<string>()
            {
                Title = databaseTitle
            }.AddChoices(databaseNames));

            // SelectionPrompts do not appear in console after selection, so print the selected value
            AnsiConsole.Markup(databaseTitle + DatabaseName);
        }


        /// <summary>
        /// Converts the model properties into a SQL connection string.
        /// </summary>
        public string ToConnectionString()
        {
            var builder = new SqlConnectionStringBuilder
            {
                IntegratedSecurity = IntegratedSecurity
            };
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


        /// <summary>
        /// Creates a new <see cref="ConnectionSettings"/> from a connection string.
        /// </summary>
        /// <param name="connectionString">A full SQL connection string.</param>
        public static ConnectionSettings FromConnectionString(string connectionString)
        {
            // Parse with the lenient base builder so keywords unknown to System.Data.SqlClient
            // (e.g. "Command Timeout") do not throw during parsing...
            var parsed = new DbConnectionStringBuilder()
            {
                ConnectionString = connectionString
            };

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

            return new ConnectionSettings()
            {
                DataSource = sqlBuilder.DataSource,
                UserID = sqlBuilder.UserID,
                Password = sqlBuilder.Password,
                DatabaseName = GetDatabaseFromConnectionString(connectionString),
                IntegratedSecurity = sqlBuilder.IntegratedSecurity
            };
        }


        /// <summary>
        /// Creates a new <see cref="ConnectionSettings"/> from interactive user prompts.
        /// </summary>
        public static ConnectionSettings FromPrompts()
        {
            const string connectionStringChoice = "Full connection string";
            const string individualFieldsChoice = "Individual fields (data source, authentication method)";
            string mode = AnsiConsole.Prompt(new SelectionPrompt<string>()
            {
                Title = $"[{Constants.PROMPT_COLOR}]How would you like to provide connection details?[/]"
            }.AddChoices(connectionStringChoice, individualFieldsChoice));

            // Create connection settings from connection string
            if (mode.Equals(connectionStringChoice, StringComparison.OrdinalIgnoreCase))
            {
                string connectionString = AnsiConsole.Prompt(new TextPrompt<string>($"[{Constants.PROMPT_COLOR}]Connection string:[/] ")
                { IsSecret = true });

                return FromConnectionString(connectionString);
            }

            // Create connection settings from prompts
            var connectionSettings = new ConnectionSettings()
            {
                DataSource = AnsiConsole.Prompt(new TextPrompt<string>($"[{Constants.PROMPT_COLOR}]Data source:[/] ")),
                IntegratedSecurity = AnsiConsole.Confirm($"[{Constants.PROMPT_COLOR}]Integrated security?:[/] ", false)
            };
            if (!connectionSettings.IntegratedSecurity)
            {
                connectionSettings.UserID = AnsiConsole.Prompt(new TextPrompt<string>($"[{Constants.PROMPT_COLOR}]User ID:[/] "));
                connectionSettings.Password = AnsiConsole.Prompt(new TextPrompt<string>($"[{Constants.PROMPT_COLOR}]Password:[/] ")
                { IsSecret = true });
            }
            connectionSettings.SetDatabaseFromPrompt();

            return connectionSettings;
        }


        private IEnumerable<string> GetDatabaseNames()
        {
            using (new CMSConnectionScope(ToConnectionString()))
            {
                string query = "SELECT name FROM master.dbo.sysdatabases WHERE name NOT IN ('master', 'tempdb', 'model', 'msdb')";
                var result = ConnectionHelper.ExecuteQuery(query, null, QueryTypeEnum.SQLQuery);
                if (result.Tables.Count == 0)
                {
                    return [];
                }

                return result.Tables[0].Rows.OfType<DataRow>().Select(r => ValidationHelper.GetString(r[0], string.Empty));
            }
        }


        /// <summary>
        /// Returns the database name parsed from <paramref name="connectionString"/>, if any.
        /// </summary>
        private static string? GetDatabaseFromConnectionString(string connectionString)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                return null;
            }

            var builder = new DbConnectionStringBuilder { ConnectionString = connectionString };
            if (builder.TryGetValue("Initial Catalog", out object? value)
                || builder.TryGetValue("Database", out value))
            {
                string name = value.ToString() ?? string.Empty;

                return string.IsNullOrEmpty(name) ? null : name;
            }

            return null;
        }
    }
}
