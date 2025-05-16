using ExpressBase.Common.Data;
using ExpressBase.Common.Messaging;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.Services;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Newtonsoft.Json;
using RabbitMQ.Client.Framing.Impl;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Collections.Generic;
using System.Data.Common;

namespace ExpressBase.MessageQueue.MQServices
{
    [Restrict(InternalOnly = true)]
    public class SMSServiceInternal : EbMqBaseService
    {
        public SMSServiceInternal(IMessageProducer _mqp) : base(_mqp)
        {
        }

        public void Post(SMSSentRequest req)
        {
            this.EbConnectionFactory = new EbConnectionFactory(req.SolnId, this.Redis);
            Dictionary<string, string> MsgStatus = this.EbConnectionFactory.SMSConnection.SendSMS(req.To, req.Body, req.Sender);

            SMSStatusLogMqRequest logMqRequest = new SMSStatusLogMqRequest
            {
                SentStatus = new SentStatus()
            };
            if (!(MsgStatus is null))
            {
                foreach (var Info in MsgStatus)
                {
                    if (Info.Key == "To")
                        logMqRequest.SentStatus.To = Info.Value;
                    if (Info.Key == "From")
                        logMqRequest.SentStatus.From = Info.Value;
                    if (Info.Key == "Body")
                        logMqRequest.SentStatus.Body = Info.Value;
                    if (Info.Key == "Status")
                        logMqRequest.SentStatus.Status = Info.Value;
                    if (Info.Key == "Result")
                        logMqRequest.SentStatus.Result = Info.Value;
                    if (Info.Key == "ConId")
                        logMqRequest.SentStatus.ConId = int.Parse(Info.Value);
                }
            }
            logMqRequest.UserId = req.UserId;
            logMqRequest.SolnId = req.SolnId;
            logMqRequest.RefId = req.RefId;
            logMqRequest.MetaData = JsonConvert.SerializeObject(req.Params);
            logMqRequest.RetryOf = req.RetryOf;
            SaveSMSLogs(logMqRequest);
        }

        public void SaveSMSLogs(SMSStatusLogMqRequest request)
        {
            EbConnectionFactory connectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);
            try
            {

                string sql = $@"INSERT INTO eb_sms_logs
                                (send_to, send_from, message_body, status, result, refid, metadata, retryof, con_id, eb_created_by, eb_created_at)
                            VALUES
                                (@to, @from, @message_body, @status, @result, @refid, @metadata, @retryof, @con_id, @user_id, {connectionFactory.DataDB.EB_CURRENT_TIMESTAMP}) RETURNING id;";

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
                throw new Exception("Exception in Sms log  Save : " + ex.Message);
            }
        }
    }
}