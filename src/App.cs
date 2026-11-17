using System.CommandLine;

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
        private const string CONNECTION_STRING_ARGNAME = "--connection-string";
        private readonly IAnonymizerService anonymizerService = anonymizerService;
        private readonly IAnonymizationTableProvider anonymizationTableProvider = anonymizationTableProvider;


        /// <summary>
        /// Runs the console application.
        /// </summary>
        /// <param name="args">Optional command-line arguments. Supports <c>--connection-string &lt;value&gt;</c> (or 
        /// <c>-c &lt;value&gt;</c>) to provide a full SQL connection string, bypassing the individual connection prompts.
        /// </param>
        public Task Run(string[] args)
        {
            var rootCommand = GetRootCommand();
            ConnectionSettings? cliConnectionSettings = null;
            rootCommand.SetAction(_ => RunInternal(cliConnectionSettings));

            var parseResult = rootCommand.Parse(args);
            if (parseResult.Errors.Any())
            {
                throw new InvalidOperationException(parseResult.Errors[0].Message);
            }

            // Parse connection string argument from command line (if provided)
            string? connectionString = parseResult.GetValue<string?>(CONNECTION_STRING_ARGNAME);
            if (!string.IsNullOrWhiteSpace(connectionString))
            {
                cliConnectionSettings = ConnectionSettings.FromConnectionString(connectionString);
            }

            return parseResult.InvokeAsync();
        }


        private async Task RunInternal(ConnectionSettings? cliConnectionSettings)
        {
            try
            {
                var tablesConfig = await anonymizationTableProvider.GetTablesConfig();
                AnsiConsole.Markup($"[{Constants.EMPHASIS_COLOR}]The anonymization process is irreversible! Please make sure you are" +
                    $" executing the process against a backup.[/]");
                AnsiConsole.WriteLine();

                var connectionSettings = GetConnectionSettings(cliConnectionSettings);

                anonymizerService.Anonymize(connectionSettings, tablesConfig);
            }
            catch (Exception ex)
            {
                AnsiConsole.WriteLine();
                AnsiConsole.WriteException(ex);
            }
        }


        private static ConnectionSettings GetConnectionSettings(ConnectionSettings? cliConnectionSettings)
        {
            var connectionSettings = cliConnectionSettings ?? ConnectionSettings.FromPrompts();

            // Database name can be empty if not provided by connection string
            if (string.IsNullOrEmpty(connectionSettings.DatabaseName))
            {
                connectionSettings.SetDatabaseFromPrompt();
            }

            return connectionSettings;
        }


        private static RootCommand GetRootCommand()
        {
            var connStringOption = new Option<string>(CONNECTION_STRING_ARGNAME, "-c");
            connStringOption.Validators.Add(result =>
            {
                string? value = result.GetValue<string?>(CONNECTION_STRING_ARGNAME);
                if (string.IsNullOrWhiteSpace(value))
                {
                    result.AddError($"Missing value for {CONNECTION_STRING_ARGNAME} argument.");
                }
            });

            return [connStringOption];
        }
    }
}
