using CMS.Commerce;
using CMS.ContactManagement;
using CMS.EmailEngine;
using CMS.Globalization;
using CMS.Membership;

namespace XperienceCommunity.DatabaseAnonymizer.Models
{
    /// <summary>
    /// Represents the physical configuration file containing table and column names to anonymize.
    /// </summary>
    internal class TablesConfiguration
    {
        /// <summary>
        /// The <see cref="TableConfiguration"/>s used during anonymization. Can be overridden via customization to the
        /// <see cref="Constants.TABLES_FILENAME"/> file.
        /// </summary>
        public IEnumerable<TableConfiguration> Tables { get; set; } = [
            new TableConfiguration() {
                TableName = "CMS_Country",
                AnonymizeColumns =
                [
                    nameof(CountryInfo.CountryName),
                    nameof(CountryInfo.CountryDisplayName),
                    nameof(CountryInfo.CountryTwoLetterCode),
                    nameof(CountryInfo.CountryThreeLetterCode),
                ]
            },
            new TableConfiguration() {
                TableName = "CMS_Email",
                AnonymizeColumns =
                [
                    nameof(EmailInfo.EmailFrom),
                    nameof(EmailInfo.EmailTo),
                    nameof(EmailInfo.EmailCc),
                    nameof(EmailInfo.EmailBcc),
                    nameof(EmailInfo.EmailSubject),
                    nameof(EmailInfo.EmailBody),
                    nameof(EmailInfo.EmailPlainTextBody),
                    nameof(EmailInfo.EmailReplyTo),
                ]
            },
            new TableConfiguration() {
                TableName = "CMS_Member",
                AnonymizeColumns =
                [
                    nameof(MemberInfo.MemberEmail),
                    nameof(MemberInfo.MemberName),
                ]
            },
            new TableConfiguration() {
                TableName = "CMS_State",
                AnonymizeColumns =
                [
                    nameof(StateInfo.StateName),
                    nameof(StateInfo.StateDisplayName),
                    nameof(StateInfo.StateCode),
                ]
            },
            new TableConfiguration() {
                TableName = "CMS_User",
                AnonymizeColumns =
                [
                    nameof(UserInfo.UserName),
                    nameof(UserInfo.FirstName),
                    nameof(UserInfo.LastName),
                    nameof(UserInfo.Email),
                ]
            },
            new TableConfiguration() {
                TableName = "Commerce_Customer",
                AnonymizeColumns =
                [
                    nameof(CustomerInfo.CustomerFirstName),
                    nameof(CustomerInfo.CustomerLastName),
                    nameof(CustomerInfo.CustomerEmail),
                    nameof(CustomerInfo.CustomerPhone),
                ]
            },
            new TableConfiguration() {
                TableName = "Commerce_CustomerAddress",
                AnonymizeColumns =
                [
                    nameof(CustomerAddressInfo.CustomerAddressFirstName),
                    nameof(CustomerAddressInfo.CustomerAddressLastName),
                    nameof(CustomerAddressInfo.CustomerAddressCompany),
                    nameof(CustomerAddressInfo.CustomerAddressEmail),
                    nameof(CustomerAddressInfo.CustomerAddressPhone),
                    nameof(CustomerAddressInfo.CustomerAddressLine1),
                    nameof(CustomerAddressInfo.CustomerAddressLine2),
                    nameof(CustomerAddressInfo.CustomerAddressCity),
                    nameof(CustomerAddressInfo.CustomerAddressZip),
                ],
                NullColumns =
                [
                    nameof(CustomerAddressInfo.CustomerAddressCountryID),
                    nameof(CustomerAddressInfo.CustomerAddressStateID),
                ]
            },
            new TableConfiguration() {
                TableName = "Commerce_OrderAddress",
                AnonymizeColumns =
                [
                    nameof(OrderAddressInfo.OrderAddressFirstName),
                    nameof(OrderAddressInfo.OrderAddressLastName),
                    nameof(OrderAddressInfo.OrderAddressCompany),
                    nameof(OrderAddressInfo.OrderAddressEmail),
                    nameof(OrderAddressInfo.OrderAddressPhone),
                    nameof(OrderAddressInfo.OrderAddressLine1),
                    nameof(OrderAddressInfo.OrderAddressLine2),
                    nameof(OrderAddressInfo.OrderAddressCity),
                    nameof(OrderAddressInfo.OrderAddressZip),
                ],
                NullColumns =
                [
                    nameof(OrderAddressInfo.OrderAddressCountryID),
                    nameof(OrderAddressInfo.OrderAddressStateID),
                ]
            },
            new TableConfiguration() {
                TableName = "OM_Contact",
                AnonymizeColumns =
                [
                    nameof(ContactInfo.ContactFirstName),
                    nameof(ContactInfo.ContactMiddleName),
                    nameof(ContactInfo.ContactLastName),
                    nameof(ContactInfo.ContactJobTitle),
                    nameof(ContactInfo.ContactAddress1),
                    nameof(ContactInfo.ContactCity),
                    nameof(ContactInfo.ContactZIP),
                    nameof(ContactInfo.ContactMobilePhone),
                    nameof(ContactInfo.ContactBusinessPhone),
                    nameof(ContactInfo.ContactEmail),
                    nameof(ContactInfo.ContactNotes),
                    nameof(ContactInfo.ContactCompanyName),
                ],
                NullColumns =
                [
                    nameof(ContactInfo.ContactCountryID),
                    nameof(ContactInfo.ContactStateID),
                    nameof(ContactInfo.ContactBirthday),
                    nameof(ContactInfo.ContactGender),
                ],
            },
        ];
    }
}
