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
        /// <param name="args">Optional command-line arguments. Supports <c>--connection-string &lt;value&gt;</c> (or 
        /// <c>-c &lt;value&gt;</c>) to provide a full SQL connection string, bypassing the individual connection prompts.
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
                ? new ConnectionSettings(connectionStringArg)
                : ConnectionSettings.FromPrompts();

            // Database name can be empty if not provided by connection string
            if (string.IsNullOrEmpty(connectionSettings.DatabaseName))
            {
                connectionSettings.SetDatabaseFromPrompt();
            }

            return connectionSettings;
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
    }
}
