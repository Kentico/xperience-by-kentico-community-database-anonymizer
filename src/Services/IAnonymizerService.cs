using XperienceCommunity.DatabaseAnonymizer.Models;

namespace XperienceCommunity.DatabaseAnonymizer.Services
{
    /// <summary>
    /// Contains methods for anonymizing database records.
    /// </summary>
    internal interface IAnonymizerService : IService
    {
        /// <summary>
        /// Runs the anonymization process.
        /// </summary>
        void Anonymize(ConnectionSettings connectionSettings, TablesConfiguration tablesConfiguration);
    }
}
