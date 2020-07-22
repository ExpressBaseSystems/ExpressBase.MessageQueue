using ExpressBase.Common.Data;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.Services;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Newtonsoft.Json;
using RabbitMQ.Client.Framing.Impl;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Data.Common;

namespace ExpressBase.MessageQueue.MQServices
{
    [Restrict(InternalOnly = true)]
    public class SMSServiceInternal : EbMqBaseService
    {
        public SMSServiceInternal(IMessageProducer _mqp) : base(_mqp)
        {
        }

        public string Post(SMSSentRequest req)
        {
            this.EbConnectionFactory = new EbConnectionFactory(req.SolnId, this.Redis);

            var MsgStatus = this.EbConnectionFactory.SMSConnection.SendSMS(req.To, req.Body);

            SMSStatusLogMqRequest logMqRequest = new SMSStatusLogMqRequest
            { SMSSentStatus = new SMSSentStatus() };

            foreach (var Info in MsgStatus)
            {
                if (Info.Key == "To")
                    logMqRequest.SMSSentStatus.To = Info.Value;
                if (Info.Key == "From")
                    logMqRequest.SMSSentStatus.From = Info.Value;
                if (Info.Key == "Body")
                    logMqRequest.SMSSentStatus.Body = Info.Value;
                if (Info.Key == "Status")
                    logMqRequest.SMSSentStatus.Status = Info.Value;
                if (Info.Key == "Result")
                    logMqRequest.SMSSentStatus.Result = Info.Value;
                if (Info.Key == "ConId")
                    logMqRequest.SMSSentStatus.ConId = int.Parse(Info.Value);
            }
            logMqRequest.UserId = req.UserId;
            logMqRequest.SolnId = req.SolnId;
            logMqRequest.RefId = req.RefId;
            logMqRequest.MetaData = JsonConvert.SerializeObject(req.Params);
            logMqRequest.RetryOf = req.RetryOf;
            SaveSMSLogs(logMqRequest);

            return null;
        }

        public void SaveSMSLogs(SMSStatusLogMqRequest request)
        {
            EbConnectionFactory connectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);
            try
            {

                string sql = @"INSERT INTO eb_sms_logs
                                (send_to, send_from, message_body, status, result, refid, metadata, retryof, con_id, eb_created_by, eb_created_at)
                            VALUES
                                (@to, @from, @message_body, @status, @result, @refid, @metadata, @retryof, @con_id, @user_id, NOW()) RETURNING id;";

                DbParameter[] parameters =
                        {
                        connectionFactory.DataDB.GetNewParameter("to",EbDbTypes.String, request.SMSSentStatus.To),
                        connectionFactory.DataDB.GetNewParameter("from",EbDbTypes.String, request.SMSSentStatus.From),
                        connectionFactory.DataDB.GetNewParameter("message_body",EbDbTypes.String, string.IsNullOrEmpty(request.SMSSentStatus.Body)?string.Empty:request.SMSSentStatus.Body),
                        connectionFactory.DataDB.GetNewParameter("status",EbDbTypes.String, string.IsNullOrEmpty(request.SMSSentStatus.Status)?string.Empty:request.SMSSentStatus.Status),
                        connectionFactory.DataDB.GetNewParameter("result", EbDbTypes.String, string.IsNullOrEmpty(request.SMSSentStatus.Result)?string.Empty:request.SMSSentStatus.Result),
                        connectionFactory.DataDB.GetNewParameter("refid", EbDbTypes.String, string.IsNullOrEmpty(request.RefId)?string.Empty:request.RefId),
                        connectionFactory.DataDB.GetNewParameter("metadata", EbDbTypes.Json, request.MetaData),
                        connectionFactory.DataDB.GetNewParameter("retryof", EbDbTypes.Int32, request.RetryOf),
                        connectionFactory.DataDB.GetNewParameter("con_id", EbDbTypes.Int32, request.SMSSentStatus.ConId),
                        connectionFactory.DataDB.GetNewParameter("user_id",EbDbTypes.Int32, request.UserId)
                        };
                var iCount = connectionFactory.DataDB.DoQuery(sql, parameters);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception in SMS Save : " + ex);
            }
        }
    }
}