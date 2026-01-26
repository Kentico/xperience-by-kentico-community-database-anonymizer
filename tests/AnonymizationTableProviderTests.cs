using XperienceCommunity.DatabaseAnonymizer.Models;
using XperienceCommunity.DatabaseAnonymizer.Services;

namespace XperienceCommunity.DatabaseAnonymizer.Tests
{
    /// <summary>
    /// Tests for <see cref="AnonymizationTableProvider"/>.
    /// </summary>
    public class AnonymizationTableProviderTests
    {
        private readonly AnonymizationTableProvider tableProvider = new();


        [TearDown]
        public void TearDown() => File.Delete(Constants.TABLES_FILENAME);


        [Test]
        public async Task GetTablesConfig_GetsDefaultConfig()
        {
            var defaultConfig = new TablesConfiguration();
            var newConfig = await tableProvider.GetTablesConfig();

            Assert.Multiple(() =>
            {
                Assert.That(File.Exists(Constants.TABLES_FILENAME), Is.True);
                Assert.That(newConfig.Tables.Count(), Is.EqualTo(defaultConfig.Tables.Count()));
            });
        }


        [Test]
        public async Task GetTablesConfig_GetsCustomConfig()
        {
            File.Copy("Data/customized_tables.json", Constants.TABLES_FILENAME);
            var config = await tableProvider.GetTablesConfig();

            Assert.That(config.Tables.Count(t => t.TableName?.Equals("My_Custom_Table") ?? false), Is.EqualTo(1));
        }
    }
}
