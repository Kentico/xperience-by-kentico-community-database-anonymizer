using CMS.Tests;

using NSubstitute;

using XperienceCommunity.DatabaseAnonymizer.Models;
using XperienceCommunity.DatabaseAnonymizer.Services;

namespace XperienceCommunity.DatabaseAnonymizer.Tests
{
    /// <summary>
    /// Tests for <see cref="App"/>.
    /// </summary>
    public class AppTests : ContainerNotBuiltUnitTests
    {
        private readonly IAnonymizerService anonymizerService = Substitute.For<IAnonymizerService>();
        private readonly IAnonymizationTableProvider anonymizationTableProvider = Substitute.For<IAnonymizationTableProvider>();
        private const string DATA_SOURCE = "localhost\\SQLEXPRESS";
        private const string DATABASE_NAME = "Kentico13";
        private const string USER_ID = "sa";
        private const string PASSWORD = "testpass";


        [Test]
        public async Task Run_ConnectionStringWithUserCredentials_SetsUserCredentials()
        {
            string connectionStringArg = $"Data Source={DATA_SOURCE};Initial Catalog={DATABASE_NAME};User ID={USER_ID};" +
                $"Password={PASSWORD};Persist Security Info=False;Connect Timeout=60;Encrypt=False;";
            var app = new App(anonymizerService, anonymizationTableProvider);

            await app.Run(["-c", connectionStringArg]);

            anonymizerService.Received().Anonymize(Arg.Is<ConnectionSettings>(cs =>
                cs.DataSource == DATA_SOURCE &&
                cs.DatabaseName == DATABASE_NAME &&
                cs.UserID == USER_ID &&
                cs.Password == PASSWORD &&
                !cs.IntegratedSecurity
            ), Arg.Any<TablesConfiguration>());
        }


        [Test]
        public async Task Run_ConnectionStringWithIntegratedSecurity_SetsIntegratedSecurity()
        {
            string connectionStringArg = $"Data Source={DATA_SOURCE};Initial Catalog={DATABASE_NAME};Integrated Security=True;" +
                "Persist Security Info=False;Connect Timeout=60;Encrypt=False;";
            var app = new App(anonymizerService, anonymizationTableProvider);

            await app.Run(["--connection-string", connectionStringArg]);

            anonymizerService.Received().Anonymize(Arg.Is<ConnectionSettings>(cs =>
                cs.DataSource == DATA_SOURCE &&
                cs.DatabaseName == DATABASE_NAME &&
                cs.IntegratedSecurity
            ), Arg.Any<TablesConfiguration>());
        }


        [TestCase(["-u"])]
        [TestCase(["--unknown-arg"])]
        [TestCase(["-c"])]
        [TestCase(["-c", ""])]
        [TestCase(["--connection-string"])]
        [TestCase(["--connection-string", ""])]
        [TestCase(["--connection-string="])]
        public async Task Run_InvalidArguments_Throws(params string[] args)
        {
            var app = new App(anonymizerService, anonymizationTableProvider);

            Assert.ThrowsAsync<InvalidOperationException>(() => app.Run(args));
        }
    }
}
