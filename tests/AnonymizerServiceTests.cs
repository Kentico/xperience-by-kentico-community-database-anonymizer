using System.Data;
using System.Text.RegularExpressions;

using CMS.Core;
using CMS.DataEngine;
using CMS.Tests;

using NSubstitute;

using XperienceCommunity.DatabaseAnonymizer.Models;
using XperienceCommunity.DatabaseAnonymizer.Services;

namespace XperienceCommunity.DatabaseAnonymizer.Tests
{
    /// <summary>
    /// Tests for <see cref="AnonymizerService"/>.
    /// </summary>
    public class AnonymizerServiceTests : ContainerNotBuiltUnitTests
    {
        private const string FAKE_TABLE = "FakeTable";
        private const string ANON_COLUMN = "Anyonymize_Me";
        private const string NULL_COLUMN = "Null_Me";
        private readonly IDataConnection dataConnection = Substitute.For<IDataConnection>();
        private readonly AnonymizerService anonymizerService = new(Substitute.For<IAnonymizationLogger>(), new TestTableManager());
        private readonly string selectQuery = $"SELECT {ANON_COLUMN}, {NULL_COLUMN}, {TestTableManager.PKEY} FROM {FAKE_TABLE}" +
            $" ORDER BY {TestTableManager.PKEY} OFFSET " + "{0}" + " ROWS FETCH NEXT 500 ROWS ONLY";
        private readonly string updateQueryPattern = $"UPDATE {FAKE_TABLE} SET {ANON_COLUMN} = '[a-zA-Z0-9]+',{NULL_COLUMN} = NULL" +
            " WHERE primary_key = {0}";
        private readonly TablesConfiguration tablesConfiguration = new()
        {
            Tables = [
                new TableConfiguration()
                {
                    TableName = FAKE_TABLE,
                    AnonymizeColumns = [ANON_COLUMN],
                    NullColumns = [NULL_COLUMN],
                }
            ]
        };


        [SetUp]
        public void SetUp()
        {
            var dataProvider = Substitute.For<IDataProvider>();
            dataProvider.CurrentConnection.Returns(dataConnection);
            dataConnection.GetExecutingConnection(Arg.Any<string>(), Arg.Any<bool>()).Returns(dataConnection);
            Service.Use<IDataProvider>(dataProvider);

            // Set up data connection to return paged results from fake table
            // FakeTable contains 5 records
            dataConnection.ExecuteQuery(string.Format(selectQuery, 0), null, QueryTypeEnum.SQLQuery, false).Returns(CreateDataSet(5));
            dataConnection.ExecuteQuery(string.Format(selectQuery, 500), null, QueryTypeEnum.SQLQuery, false).Returns(CreateDataSet(0));

            Service.InitializeContainer();
        }


        [Test]
        public void Anonymize_SelectsPagedResults()
        {
            anonymizerService.Anonymize(new ConnectionSettings(), tablesConfiguration);

            Assert.Multiple(() =>
            {
                dataConnection.Received().ExecuteQuery(string.Format(selectQuery, 0), null, QueryTypeEnum.SQLQuery, false);
                dataConnection.Received().ExecuteQuery(string.Format(selectQuery, 500), null, QueryTypeEnum.SQLQuery, false);
                dataConnection.DidNotReceive().ExecuteQuery(string.Format(selectQuery, 1000), null, QueryTypeEnum.SQLQuery, false);
            });
        }


        [Test]
        public void Anonymize_AnonymizesAndNullsColumns()
        {
            // Anonymizer should run 5 update queries for rows in FakeTable
            string expectedUpdateQuery = string.Format(updateQueryPattern, 0) +
                $"{Environment.NewLine}{string.Format(updateQueryPattern, 1)}" +
                $"{Environment.NewLine}{string.Format(updateQueryPattern, 2)}" +
                $"{Environment.NewLine}{string.Format(updateQueryPattern, 3)}" +
                $"{Environment.NewLine}{string.Format(updateQueryPattern, 4)}";

            anonymizerService.Anonymize(new ConnectionSettings(), tablesConfiguration);

            dataConnection.Received().ExecuteNonQuery(
                Arg.Is<string>(s => Regex.IsMatch(s, expectedUpdateQuery)),
                null,
                QueryTypeEnum.SQLQuery,
                false);
        }


        private static DataSet CreateDataSet(int numRows)
        {
            var dataset = new DataSet();

            var resultTable = new DataTable();
            resultTable.Columns.Add(TestTableManager.PKEY, typeof(int));
            resultTable.Columns.Add(ANON_COLUMN, typeof(string));
            resultTable.Columns.Add(NULL_COLUMN, typeof(string));

            // Add rows- primary key starts a 0
            for (int i = 0; i < numRows; i++)
            {
                resultTable.Rows.Add(i, $"anon_value_{i}", $"null_value_{i}");
            }

            dataset.Tables.Add(resultTable);

            return dataset;
        }
    }


    public class TestTableManager : AbstractTableManager
    {
        public const string PKEY = "primary_key";


        public override bool TableExists(string tableName) => true;


        public override List<string> GetPrimaryKeyColumns(string tableName) => [PKEY];
    }
}
