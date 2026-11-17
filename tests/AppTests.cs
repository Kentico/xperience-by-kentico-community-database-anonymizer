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


        [TearDown]
        public void TearDown() => anonymizerService.ClearReceivedCalls();


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


        // Database can be omitted from connection string to be selected from prompt later
        [TestCase(["-c", "Data Source=localhost;Integrated Security=True;"])]
        [TestCase(["--connection-string", "Data Source=localhost;Initial Catalog=xperience;User ID=sa;Password=test"])]
        [TestCase(["--connection-string=Data Source=localhost;Initial Catalog=xperience;Integrated Security=True;"])]
        // "Command Timeout" is only supported by Microsoft.Data.SqlClient, but is a valid keyword
        [TestCase(["-c", "Data Source=localhost;Initial Catalog=xperience;Integrated Security=True;Command Timeout=60;"])]
        public async Task Run_ValidArguments_DoesNotThrow(params string[] args)
        {
            var app = new App(anonymizerService, anonymizationTableProvider);

            Assert.DoesNotThrowAsync(() => app.Run(args));
        }


        [TestCase(["-u"])]
        [TestCase(["--unknown-arg"])]
        [TestCase(["-c"])]
        [TestCase(["-c", ""])]
        [TestCase(["--connection-string"])]
        [TestCase(["--connection-string", ""])]
        [TestCase(["--connection-string="])]
        public async Task Run_InvalidArguments_ThrowsInvalidOperationException(params string[] args)
        {
            var app = new App(anonymizerService, anonymizationTableProvider);

            Assert.ThrowsAsync<InvalidOperationException>(() => app.Run(args));
            anonymizerService.DidNotReceiveWithAnyArgs().Anonymize(default!, default!);
        }


        [TestCase(["-c", "somerandomtext"])]
        [TestCase(["-c", "Initial Catalog=xperience;Integrated Security=True;"])]
        [TestCase(["-c", "Data Source=localhost;Initial Catalog=xperience;"])]
        [TestCase(["-c", "Data Source=localhost;Initial Catalog=xperience;Integrated Security=True;InvalidParameter=xxx"])]
        [TestCase(["-c", "Data Source=localhost;Initial Catalog=xperience;Integrated Security=notabool;"])]
        [TestCase(["-c", "Data Source=localhost;Initial Catalog=xperience;Password=testpass;"])]
        public async Task Run_MalformedConnectionString_ThrowsArgumentException(params string[] args)
        {
            var app = new App(anonymizerService, anonymizationTableProvider);

            Assert.ThrowsAsync<ArgumentException>(() => app.Run(args));
            anonymizerService.DidNotReceiveWithAnyArgs().Anonymize(default!, default!);
        }
    }
}
