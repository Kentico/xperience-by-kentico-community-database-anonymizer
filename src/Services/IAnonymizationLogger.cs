namespace XperienceCommunity.DatabaseAnonymizer.Services
{
    /// <summary>
    /// Contains methods for logging anonymization operations.
    /// </summary>
    internal interface IAnonymizationLogger : IService
    {
        /// <summary>
        /// Logs information regarding the end of anonymization.
        /// </summary>
        void LogEnd();


        /// <summary>
        /// Logs an error.
        /// </summary>
        /// <param name="message">The error to log.</param>
        void LogError(string message);


        /// <summary>
        /// Updates the number of rows modified for a table.
        /// </summary>
        /// <param name="table">The database table.</param>
        /// <param name="rowsModified">The number of rows modified.</param>
        void LogModification(string table, int rowsModified);


        /// <summary>
        /// Logs information regarding the start of anonymization.
        /// </summary>
        void LogStart();


        /// <summary>
        /// Logs information after a table has finished processing.
        /// </summary>
        /// <param name="tableName">The name of the processed table.</param>
        void LogTableEnd(string tableName);


        /// <summary>
        /// Logs information before a table is processed.
        /// </summary>
        /// <param name="tableName">The name of the table to process.</param>
        void LogTableStart(string tableName);
    }
}
