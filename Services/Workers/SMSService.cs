using ExpressBase.Common.Data;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.Services;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using ServiceStack.Messaging;
using System.Data.Common;

namespace ExpressBase.MessageQueue.MQServices
{
    [Restrict(InternalOnly = true)]
    public class SMSServiceInternal : EbMqBaseService
    {
        public SMSServiceInternal(IMessageProducer _mqp) : base(_mqp)
        {
        }

        public string Post(SMSSentMqRequest req)
        {
            this.EbConnectionFactory = new EbConnectionFactory(req.SolnId, this.Redis);

            var MsgStatus = this.EbConnectionFactory.SMSConnection.SendSMS(req.To, req.From, req.Body);

            //SMSStatusLogMqRequest logMqRequest = new SMSStatusLogMqRequest
            //{ SMSSentStatus = new SMSSentStatus()};

            //foreach (var Info in MsgStatus)
            //{
            //    if (Info.Key == "To")
            //        logMqRequest.SMSSentStatus.To = Info.Value;
            //    if (Info.Key == "From")
            //        logMqRequest.SMSSentStatus.From = Info.Value;
            //    if (Info.Key == "Uri")
            //        logMqRequest.SMSSentStatus.Uri = Info.Value;
            //    if (Info.Key == "Body")
            //        logMqRequest.SMSSentStatus.Body = Info.Value;
            //    if (Info.Key == "Status")
            //        logMqRequest.SMSSentStatus.Status = Info.Value;
            //    if (Info.Key == "SentTime")
            //        //logMqRequest.SMSSentStatus.SentTime = DateTime.Parse(Info.Value);
            //        if (Info.Key == "ErrorMessage")
            //            logMqRequest.SMSSentStatus.ErrorMessage = Info.Value;
            //}
            //logMqRequest.UserId = req.UserId;
            //logMqRequest.SolnId = req.SolnId;

           // this.MessageProducer3.Publish(logMqRequest);
            return null;
        }

        public string Post(SMSStatusLogMqRequest request)
        {
            EbConnectionFactory connectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);

            string sql = "INSERT INTO logs_sms(uri, send_to, send_from, message_body, status, error_message, user_id, context_id) VALUES (@uri, @to, @from, @message_body, @status, @error_message, @user_id, @context_id) RETURNING id";

            DbParameter[] parameters =
                    {
                        connectionFactory.DataDB.GetNewParameter("uri", EbDbTypes.String, string.IsNullOrEmpty(request.SMSSentStatus.Uri)?string.Empty:request.SMSSentStatus.Uri),
                        connectionFactory.DataDB.GetNewParameter("to",EbDbTypes.String, request.SMSSentStatus.To),
                        connectionFactory.DataDB.GetNewParameter("from",EbDbTypes.String, request.SMSSentStatus.From),
                        connectionFactory.DataDB.GetNewParameter("message_body",EbDbTypes.String, string.IsNullOrEmpty(request.SMSSentStatus.Body)?string.Empty:request.SMSSentStatus.Body),
                        connectionFactory.DataDB.GetNewParameter("status",EbDbTypes.String, string.IsNullOrEmpty(request.SMSSentStatus.Status)?string.Empty:request.SMSSentStatus.Status),
                        //connectionFactory.DataDB.GetNewParameter("sent_time",System.Data.DbType.DateTime, request.SMSSentStatus.SentTime),
                        connectionFactory.DataDB.GetNewParameter("error_message",EbDbTypes.String, string.IsNullOrEmpty(request.SMSSentStatus.ErrorMessage)?string.Empty:request.SMSSentStatus.ErrorMessage),
                        connectionFactory.DataDB.GetNewParameter("user_id",EbDbTypes.Int32, request.UserId),
                        connectionFactory.DataDB.GetNewParameter("context_id",EbDbTypes.Int32, string.IsNullOrEmpty(request.ContextId.ToString())?request.UserId:request.ContextId)
                        };
            var iCount = connectionFactory.DataDB.DoQuery(sql, parameters);

            return null;
        }
    }
}