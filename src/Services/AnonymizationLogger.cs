using System.Diagnostics;

using CMS;
using CMS.Helpers;

using Spectre.Console;

using XperienceCommunity.DatabaseAnonymizer.Services;

[assembly: RegisterImplementation(typeof(IAnonymizationLogger), typeof(AnonymizationLogger))]
namespace XperienceCommunity.DatabaseAnonymizer.Services
{
    public class AnonymizationLogger : IAnonymizationLogger
    {
        private readonly Stopwatch mStopwatch = new();
        private readonly Dictionary<string, int> mModifications = [];


        public void LogEnd()
        {
            mStopwatch.Stop();
            double seconds = mStopwatch.ElapsedMilliseconds / (double)1000;
            int minutes = ValidationHelper.GetInteger(Math.Ceiling(seconds / 60), 0);
            AnsiConsole.MarkupLine($"[{Constants.SUCCESS_COLOR}]Finished successfully at {DateTime.Now:d}" +
                $" {DateTime.Now:t} ({minutes} minutes)[/]");
        }


        public void LogError(string message) => AnsiConsole.MarkupLine($"[{Constants.ERROR_COLOR}]{message}[/]");


        public void LogModification(string table, int rowsModified)
        {
            if (rowsModified <= 0)
            {
                return;
            }

            if (mModifications.TryAdd(table, rowsModified))
            {
                return;
            }

            int modifications = mModifications[table];
            mModifications[table] = modifications + rowsModified;
        }


        public void LogTableEnd(string tableName)
        {
            if (!mModifications.TryGetValue(tableName, out int modifications))
            {
                modifications = 0;
            }

            AnsiConsole.MarkupLine($"Anonymized {modifications} rows in {tableName}");
        }


        public void LogTableStart(string tableName) => AnsiConsole.MarkupLine($"Processing table {tableName}...");


        public void LogStart()
        {
            mStopwatch.Start();
            AnsiConsole.WriteLine();
            AnsiConsole.MarkupLine($"[{Constants.EMPHASIS_COLOR}]Anonymization process started at {DateTime.Now:d}" +
                $" {DateTime.Now:t}[/]");
        }
    }
}
