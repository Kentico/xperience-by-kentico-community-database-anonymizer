using System.Data;

using CMS.DataEngine;
using CMS.Helpers;

using Spectre.Console;

using XperienceCommunity.DatabaseAnonymizer.Models;
using XperienceCommunity.DatabaseAnonymizer.Services;

namespace XperienceCommunity.DatabaseAnonymizer
{
    /// <summary>
    /// The main entry point for the console application which supports Dependency Injection.
    /// </summary>
    internal class App(IAnonymizerService anonymizerService, IAnonymizationTableProvider anonymizationTableProvider)
    {
        private readonly IAnonymizerService anonymizerService = anonymizerService;
        private readonly IAnonymizationTableProvider anonymizationTableProvider = anonymizationTableProvider;


        /// <summary>
        /// Runs the console application.
        /// </summary>
        /// <param name="args">
        /// Optional command-line arguments. Supports <c>--connection-string &lt;value&gt;</c>
        /// (or <c>-c &lt;value&gt;</c>) to provide a full SQL connection string, bypassing
        /// the individual connection prompts.
        /// </param>
        public async Task Run(string[]? args = null)
        {
            try
            {
                var tablesConfig = await anonymizationTableProvider.GetTablesConfig();
                AnsiConsole.Markup($"[{Constants.EMPHASIS_COLOR}]The anonymization process is irreversible! Please make sure you are" +
                    $" executing the process against a backup.[/]");
                AnsiConsole.WriteLine();
                var connectionSettings = GetConnectionSettings(args);
                anonymizerService.Anonymize(connectionSettings, tablesConfig);
            }
            catch (Exception ex)
            {
                AnsiConsole.WriteLine();
                AnsiConsole.WriteException(ex);
            }
        }


        private static ConnectionSettings GetConnectionSettings(string[]? args)
        {
            string? connectionStringArg = TryGetConnectionStringArg(args);
            var connectionSettings = connectionStringArg is not null
                ? new ConnectionSettings { ConnectionString = connectionStringArg }
                : PromptConnectionSettings();

            // If the database name is already provided (via connection string or individual prompts), skip selection.
            if (!string.IsNullOrEmpty(connectionSettings.DatabaseName)
                || !string.IsNullOrEmpty(connectionSettings.GetDatabaseFromConnectionString()))
            {
                return connectionSettings;
            }

            var databaseNames = GetDatabaseNames(connectionSettings);
            if (!databaseNames.Any())
            {
                throw new InvalidOperationException("Failed to retrieve databases from server");
            }

            string databaseTitle = $"[{Constants.PROMPT_COLOR}]Database:[/] ";
            connectionSettings.DatabaseName = AnsiConsole.Prompt(new SelectionPrompt<string>()
            {
                Title = databaseTitle
            }.AddChoices(databaseNames));
            // SelectionPrompts do not appear in console after selection, so print the selected value
            AnsiConsole.Markup(databaseTitle + connectionSettings.DatabaseName);

            return connectionSettings;
        }


        private static ConnectionSettings PromptConnectionSettings()
        {
            const string connectionStringChoice = "Full connection string";
            const string individualFieldsChoice = "Individual fields (data source, user, password)";
            string mode = AnsiConsole.Prompt(new SelectionPrompt<string>()
            {
                Title = $"[{Constants.PROMPT_COLOR}]How would you like to provide connection details?[/]"
            }.AddChoices(connectionStringChoice, individualFieldsChoice));

            if (mode == connectionStringChoice)
            {
                string connectionString = AnsiConsole.Prompt(new TextPrompt<string>(
                    $"[{Constants.PROMPT_COLOR}]Connection string:[/] ")
                { IsSecret = true });
                // Validate format early with a clear error message. Use the lenient base builder
                // so keywords unknown to System.Data.SqlClient (e.g. "Command Timeout") are accepted.
                _ = new System.Data.Common.DbConnectionStringBuilder { ConnectionString = connectionString };

                return new ConnectionSettings { ConnectionString = connectionString };
            }

            return new ConnectionSettings()
            {
                DataSource = AnsiConsole.Prompt(new TextPrompt<string>($"[{Constants.PROMPT_COLOR}]Data source:[/] ")),
                UserID = AnsiConsole.Prompt(new TextPrompt<string>($"[{Constants.PROMPT_COLOR}]User ID:[/] ")),
                Password = AnsiConsole.Prompt(new TextPrompt<string>($"[{Constants.PROMPT_COLOR}]Password:[/] ") { IsSecret = true })
            };
        }


        private static string? TryGetConnectionStringArg(string[]? args)
        {
            if (args is null || args.Length == 0)
            {
                return null;
            }

            for (int i = 0; i < args.Length; i++)
            {
                string arg = args[i];
                if (string.Equals(arg, "--connection-string", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(arg, "-c", StringComparison.OrdinalIgnoreCase))
                {
                    if (i + 1 >= args.Length)
                    {
                        throw new ArgumentException($"Missing value for '{arg}' argument.");
                    }

                    return args[i + 1];
                }

                const string inlinePrefix = "--connection-string=";
                if (arg.StartsWith(inlinePrefix, StringComparison.OrdinalIgnoreCase))
                {
                    return arg[inlinePrefix.Length..];
                }
            }

            return null;
        }


        private static IEnumerable<string> GetDatabaseNames(ConnectionSettings connectionSettings)
        {
            using (new CMSConnectionScope(connectionSettings.ToConnectionString()))
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
    }
}
