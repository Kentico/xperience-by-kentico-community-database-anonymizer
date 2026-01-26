using System.Data;
using System.Security.Cryptography;
using System.Text;

using CMS;
using CMS.DataEngine;
using CMS.Helpers;
using CMS.Membership;

using XperienceCommunity.DatabaseAnonymizer.Models;
using XperienceCommunity.DatabaseAnonymizer.Services;

[assembly: RegisterImplementation(typeof(IAnonymizerService), typeof(AnonymizerService))]
namespace XperienceCommunity.DatabaseAnonymizer.Services
{
    public class AnonymizerService(IAnonymizationLogger anonmyzationLogger, ITableManager tableManager) : IAnonymizerService
    {
        private const int BATCH_SIZE = 500;
        private readonly IAnonymizationLogger anonymizationLogger = anonmyzationLogger;
        private readonly char[] chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890".ToCharArray();


        public void Anonymize(ConnectionSettings connectionSettings, TablesConfiguration tablesConfiguration)
        {
            anonymizationLogger.LogStart();
            ConnectionHelper.ConnectionString = connectionSettings.ToConnectionString();
            foreach (var table in tablesConfiguration.Tables)
            {
                AnonymizeTable(table);
            }

            anonymizationLogger.LogEnd();
        }


        private void AnonymizeTable(TableConfiguration table)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(table.TableName);
            anonymizationLogger.LogTableStart(table.TableName);
            if (!tableManager.TableExists(table.TableName))
            {
                anonymizationLogger.LogError($"Skipped nonexistent table {table.TableName}");

                return;
            }

            if (!table.AnonymizeColumns.Any() && !table.NullColumns.Any())
            {
                anonymizationLogger.LogError($"Skipped table {table.TableName} with no columns");

                return;
            }

            var identityColumns = tableManager.GetPrimaryKeyColumns(table.TableName);
            if (identityColumns.Count == 0)
            {
                anonymizationLogger.LogError($"Skipped table {table.TableName} with no identity columns");

                return;
            }

            int currentPage = 0;
            IEnumerable<DataRow> rows;
            do
            {
                rows = GetPagedResult(table, identityColumns, currentPage);
                var updateStatements = rows.Select(r => GetUpdateRowStatement(r, table, identityColumns));
                if (updateStatements.Any())
                {
                    string query = string.Join(Environment.NewLine, updateStatements);
                    int rowsModified = ConnectionHelper.ExecuteNonQuery(query, null, QueryTypeEnum.SQLQuery);
                    anonymizationLogger.LogModification(table.TableName, rowsModified);
                }

                currentPage++;
            }
            while (rows.Any());
            anonymizationLogger.LogTableEnd(table.TableName);
        }


        private string AnonymizeValue(string value)
        {
            int size = value.Length;
            byte[] data = new byte[4 * size];
            using (var crypto = RandomNumberGenerator.Create())
            {
                crypto.GetBytes(data);
            }
            var result = new StringBuilder(size);
            for (int i = 0; i < size; i++)
            {
                uint rnd = BitConverter.ToUInt32(data, i * 4);
                long idx = rnd % chars.Length;

                result.Append(chars[idx]);
            }

            return result.ToString();
        }


        /// <summary>
        /// Gets paged records from a database table.
        /// </summary>
        /// <param name="table">The configuration of the current table being processed.</param>
        /// <param name="identityColumns">The identity columns of the current table.</param>
        /// <param name="currentPage">The page to retrieve.</param>
        private static IEnumerable<DataRow> GetPagedResult(TableConfiguration table, IEnumerable<string> identityColumns, int currentPage)
        {
            int offset = currentPage * BATCH_SIZE;
            var selectColumns = table.AnonymizeColumns.Union(table.NullColumns).Union(identityColumns);
            string? orderColumn = identityColumns.FirstOrDefault();
            if (orderColumn is null)
            {
                return [];
            }

            string queryText = $"SELECT {string.Join(", ", selectColumns)} FROM {table.TableName} ORDER BY {orderColumn} OFFSET {offset}" +
                $" ROWS FETCH NEXT {BATCH_SIZE} ROWS ONLY";
            var result = ConnectionHelper.ExecuteQuery(queryText, null, QueryTypeEnum.SQLQuery);
            if (result.Tables.Count == 0)
            {
                return [];
            }

            return result.Tables[0].Rows.OfType<DataRow>();
        }


        /// <summary>
        /// Gets a SQL UPDATE statement used to anonymize and null the columns of a record.
        /// </summary>
        /// <param name="row">The record to update.</param>
        /// <param name="tableConfiguration">The configuration of the current table being processed.</param>
        /// <param name="identityColumns">The identity columns of the table.</param>
        private string GetUpdateRowStatement(
            DataRow row,
            TableConfiguration tableConfiguration,
            IEnumerable<string> identityColumns)
        {
            var setStatements = new List<string>();
            // Process anonymize columns
            foreach (string column in tableConfiguration.AnonymizeColumns)
            {
                object currentValue = row[column];
                string currentValueString = ValidationHelper.GetString(currentValue, string.Empty);
                if (SkipProcessing(currentValueString, column))
                {
                    continue;
                }

                string newValue = AnonymizeValue(currentValueString);
                setStatements.Add($"{column} = '{newValue}'");
            }

            // Process null columns
            foreach (string column in tableConfiguration.NullColumns)
            {
                object currentValue = row[column];
                string currentValueString = ValidationHelper.GetString(currentValue, string.Empty);
                if (SkipProcessing(currentValueString, column))
                {
                    continue;
                }

                setStatements.Add($"{column} = NULL");
            }

            if (setStatements.Count == 0)
            {
                return string.Empty;
            }

            var where = identityColumns.Select(col => $"{col} = {row[col]}");

            return $"UPDATE {tableConfiguration.TableName} SET {string.Join(",", setStatements)} WHERE {string.Join(" AND ", where)}";
        }


        /// <summary>
        /// Returns true if the column should not be anonymized or nulled. Always returns <c>true</c> for null or empty value.
        /// </summary>
        /// <param name="value">The current value of the column.</param>
        /// <param name="column">The column name.</param>
        private static bool SkipProcessing(string value, string column)
        {
            if (string.IsNullOrEmpty(value))
            {
                return true;
            }

            if (column.Equals(nameof(UserInfo.UserName), StringComparison.InvariantCultureIgnoreCase) &&
                (value.Equals("administrator", StringComparison.InvariantCultureIgnoreCase) ||
                 value.Equals("kentico-system-service", StringComparison.InvariantCultureIgnoreCase) ||
                 value.Equals("public", StringComparison.InvariantCultureIgnoreCase)))
            {
                return true;
            }

            return false;
        }
    }
}
