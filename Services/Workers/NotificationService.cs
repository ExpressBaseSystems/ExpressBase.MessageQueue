using ExpressBase.Common.Data;
using ExpressBase.Common.ServerEvents_Artifacts;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.Services;
using ServiceStack.Messaging;
using System;
using System.Data.Common;

namespace ExpressBase.MessageQueue.Services.Workers
{
    public class NotificationService : EbMqBaseService
    {
        public NotificationService(IMessageProducer _mqp) : base(_mqp)
        {

        }

        public void Post(NotificationToDBRequest request)
        {
            try
            {
                this.EbConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);

                string str = @"INSERT INTO eb_notifications (notification_id, user_id, notification)
                                                  VALUES (:notification_id, :user_id, :notification)";

                DbParameter[] parameters = {
                    this.EbConnectionFactory.DataDB.GetNewParameter("notification_id", EbDbTypes.String, request.NotificationId),
                    this.EbConnectionFactory.DataDB.GetNewParameter("user_id", EbDbTypes.Int32, request.NotifyUserId),
                    this.EbConnectionFactory.DataDB.GetNewParameter("notification", EbDbTypes.Json, request.Notification)
                };
                this.EbConnectionFactory.DataDB.DoQuery(str, parameters);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Mq.NotificationToDBRequest--" + ex.Message);
            }
        }
    }
}
