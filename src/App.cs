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
        public async Task Run()
        {
            try
            {
                var tablesConfig = await anonymizationTableProvider.GetTablesConfig();
                AnsiConsole.Markup($"[{Constants.EMPHASIS_COLOR}]The anonymization process is irreversible! Please make sure you are" +
                    $" executing the process against a backup.[/]");
                AnsiConsole.WriteLine();
                var connectionSettings = GetConnectionSettings();
                anonymizerService.Anonymize(connectionSettings, tablesConfig);
            }
            catch (Exception ex)
            {
                AnsiConsole.WriteLine();
                AnsiConsole.WriteException(ex);
            }
        }


        private static ConnectionSettings GetConnectionSettings()
        {
            var connectionSettings = new ConnectionSettings()
            {
                DataSource = AnsiConsole.Prompt(new TextPrompt<string>($"[{Constants.PROMPT_COLOR}]Data source:[/] ")),
                UserID = AnsiConsole.Prompt(new TextPrompt<string>($"[{Constants.PROMPT_COLOR}]User ID:[/] ")),
                Password = AnsiConsole.Prompt(new TextPrompt<string>($"[{Constants.PROMPT_COLOR}]Password:[/] ") { IsSecret = true })
            };
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
