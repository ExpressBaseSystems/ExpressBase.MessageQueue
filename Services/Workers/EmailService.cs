using ExpressBase.Common.ServiceClients;
using ExpressBase.Objects.ServiceStack_Artifacts;
using MailKit.Net.Smtp;
using MimeKit;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.MessageQueue.Services.Workers
{
    [Restrict(InternalOnly = true)]
    public class EmailServiceInternal : EbMqBaseService
    {
        public EmailServiceInternal(IMessageProducer _mqp, IMessageQueueClient _mqc, IEbServerEventClient _sec) : base(_mqp, _mqc, _sec) { }

        public string Post(EmailServicesRequest request)
        {
            var emailMessage = new MimeMessage();
            emailMessage.From.Add(new MailboxAddress("EXPRESSbase", "info@expressbase.com"));
            emailMessage.To.Add(new MailboxAddress("", request.To));
            emailMessage.Subject = request.Subject;
            emailMessage.Body = new TextPart("html") { Text = request.Message };
            try
            {
                using (var client = new SmtpClient())
                {// after completing connection manager implementation..take all credentials from connection object
                    client.LocalDomain = "www.expressbase.com";
                    client.Connect("smtp.gmail.com", 465, true);
                    client.Authenticate(new System.Net.NetworkCredential() { UserName = "expressbasesystems@gmail.com", Password = "ebsystems" });
                    client.Send(emailMessage);
                    client.Disconnect(true);
                }
            }
            catch (Exception e)
            {
                return e.Message;
            }
            return null;
        }
    }
}
