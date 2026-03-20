using XperienceCommunity.DatabaseAnonymizer.Models;

namespace XperienceCommunity.DatabaseAnonymizer.Services
{
    /// <summary>
    /// Contains methods for retrieving the data which should be anonymized.
    /// </summary>
    internal interface IAnonymizationTableProvider : IService
    {
        /// <summary>
        /// Gets the configuration containing tables and columns to anonymize.
        /// </summary>
        Task<TablesConfiguration> GetTablesConfig();
    }
}
