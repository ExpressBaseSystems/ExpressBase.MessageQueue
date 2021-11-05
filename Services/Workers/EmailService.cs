using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.Messaging;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.Services;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Newtonsoft.Json;
using ServiceStack;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.MessageQueue.Services.Workers
{

    [Restrict(InternalOnly = true)]
    public class EmailInternalService : EbMqBaseService
    {
        public EmailInternalService() : base() { }

        public void Post(EmailServicesRequest request)
        {
            SentStatus _sentStatus;
            if (request.SolnId == CoreConstants.EXPRESSBASE)
            {
                _sentStatus = this.InfraConnectionFactory.EmailConnection.Send(request.To, request.Subject, request.Message, request.Cc, request.Bcc, request.AttachmentReport, request.AttachmentName, request.ReplyTo);
            }
            else
            {
                base.EbConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);
                if (this.EbConnectionFactory.EmailConnection != null)
                {
                    _sentStatus = this.EbConnectionFactory.EmailConnection.Send(request.To, request.Subject, request.Message, request.Cc, request.Bcc, request.AttachmentReport, request.AttachmentName, request.ReplyTo);
                    Console.WriteLine("Inside EmailService/EmailServiceInternal in SS \n After Email \nSend To:" + request.To);
                }
                else
                {
                    throw new Exception("Email Connection not set for " + request.SolnId);
                }
            }
            EmailStatusLogMqRequest logMqRequest = new EmailStatusLogMqRequest
            {
                SentStatus = _sentStatus
            }; 

            logMqRequest.UserId = request.UserId;
            logMqRequest.SolnId = request.SolnId;
            logMqRequest.RefId = request.RefId;
            logMqRequest.MetaData = JsonConvert.SerializeObject(request.Params);
            logMqRequest.RetryOf = request.RetryOf;
            SaveEmailLogs(logMqRequest);
        }
        public void SaveEmailLogs(EmailStatusLogMqRequest request)
        {
            EbConnectionFactory connectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);
            try
            {

                string sql = @"INSERT INTO eb_email_logs
                                (send_to, send_from, message_body, status, result, refid, metadata, retryof, con_id, eb_created_by, eb_created_at)
                            VALUES
                                (@to, @from, @message_body, @status, @result, @refid, @metadata, @retryof, @con_id, @user_id, NOW()) RETURNING id;";

                DbParameter[] parameters =
                        {
                        connectionFactory.DataDB.GetNewParameter("to",EbDbTypes.String, request.SentStatus.To),
                        connectionFactory.DataDB.GetNewParameter("from",EbDbTypes.String, request.SentStatus.From),
                        connectionFactory.DataDB.GetNewParameter("message_body",EbDbTypes.String, string.IsNullOrEmpty(request.SentStatus.Body)?string.Empty:request.SentStatus.Body),
                        connectionFactory.DataDB.GetNewParameter("status",EbDbTypes.String, string.IsNullOrEmpty(request.SentStatus.Status)?string.Empty:request.SentStatus.Status),
                        connectionFactory.DataDB.GetNewParameter("result", EbDbTypes.String, string.IsNullOrEmpty(request.SentStatus.Result)?string.Empty:request.SentStatus.Result),
                        connectionFactory.DataDB.GetNewParameter("refid", EbDbTypes.String, string.IsNullOrEmpty(request.RefId)?string.Empty:request.RefId),
                        connectionFactory.DataDB.GetNewParameter("metadata", EbDbTypes.Json, request.MetaData),
                        connectionFactory.DataDB.GetNewParameter("retryof", EbDbTypes.Int32, request.RetryOf),
                        connectionFactory.DataDB.GetNewParameter("con_id", EbDbTypes.Int32, request.SentStatus.ConId),
                        connectionFactory.DataDB.GetNewParameter("user_id",EbDbTypes.Int32, request.UserId)
                        };
                var iCount = connectionFactory.DataDB.DoQuery(sql, parameters);
            }
            catch (Exception ex)
            {
                throw new Exception("Exception in Email log  Save : " + ex.Message);
            }
        }

    }
}