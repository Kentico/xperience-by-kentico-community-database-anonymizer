using System.Data;
using System.Data.SqlClient;

using CMS.DataEngine;

using CMS.Helpers;

using Spectre.Console;

using MicrosoftSqlConnectionStringBuilder = Microsoft.Data.SqlClient.SqlConnectionStringBuilder;

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
                throw new InvalidOperationException("Failed to retrieve databases from server. Check the connection settings and try again.");
            }

            string databaseTitle = $"[{Constants.PROMPT_COLOR}]Database:[/] ";
            DatabaseName = AnsiConsole.Prompt(new SelectionPrompt<string>()
            {
                Title = databaseTitle
            }.AddChoices(databaseNames));

            // SelectionPrompts do not appear in console after selection, so print the selected value
            AnsiConsole.Markup(databaseTitle + Markup.Escape(DatabaseName ?? string.Empty));
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
        /// <exception cref="ArgumentException">Thrown when <paramref name="connectionString"/> is malformed, contains an
        /// unrecognized keyword, or is missing the data source or authentication details.</exception>
        public static ConnectionSettings FromConnectionString(string connectionString)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

            // Parse with the Microsoft.Data.SqlClient builder, which throws ArgumentException for malformed strings and
            // unrecognized keywords, but still accepts keywords newer than System.Data.SqlClient (e.g. "Command Timeout").
            // Only the properties below are kept, so unsupported keywords never reach the System.Data.SqlClient provider.
            MicrosoftSqlConnectionStringBuilder builder;
            try
            {
                builder = new MicrosoftSqlConnectionStringBuilder(connectionString);
            }
            catch (Exception ex) when (ex is ArgumentException or FormatException or InvalidOperationException)
            {
                throw new ArgumentException($"The connection string is invalid: {ex.Message}", nameof(connectionString), ex);
            }

            if (string.IsNullOrWhiteSpace(builder.DataSource))
            {
                throw new ArgumentException("The connection string must specify a data source.", nameof(connectionString));
            }

            if (!builder.IntegratedSecurity && string.IsNullOrWhiteSpace(builder.UserID))
            {
                throw new ArgumentException("The connection string must specify either 'Integrated Security=True' or a 'User ID'.",
                    nameof(connectionString));
            }

            return new ConnectionSettings()
            {
                DataSource = builder.DataSource,
                UserID = builder.UserID,
                Password = builder.Password,
                DatabaseName = builder.InitialCatalog,
                IntegratedSecurity = builder.IntegratedSecurity
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
            try
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
            catch
            {
                return [];
            }
        }
    }
}
