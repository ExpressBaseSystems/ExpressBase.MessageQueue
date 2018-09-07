using ExpressBase.Common.Constants;
using ExpressBase.Common.ServiceClients;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.Security;
using MailKit.Net.Smtp;
using MimeKit;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Mail;
using System.Threading.Tasks;

namespace ExpressBase.MessageQueue.Services.Workers
{
    [Restrict(InternalOnly = true)]
    public class EmailInternalService : EbMqBaseService
    {
        public EmailInternalService(IServiceClient _ssclient) : base(_ssclient) { }

        public string Post(EmailServicesRequest request)
        {
            try
            {
                ServiceStackClient.RefreshToken = "eyJ0eXAiOiJKV1RSIiwiYWxnIjoiUlMyNTYiLCJraWQiOiJpcDQifQ.eyJzdWIiOiJlYmRibGx6MjNua3FkNjIwMTgwMjIwMTIwMDMwOmJpbml2YXJnaGVzZUBnbWFpbC5jb206ZGMiLCJpYXQiOjE1MzYyOTU1OTcsImV4cCI6MTUzNjM4MTk5N30.IJp7gOSrCy7Nimr8W_AnHXFXcNfAKFKY_ntt97Elwd5E-84EL9PhULE2XiF5-64zr1dGyoYOPFlEdkWD0lOPW8g-UpqK3ycrIDBtrDcvxtR-DhV_1aXnnh3H-O5zCCsyDiF8yBH9rDiPtgjt9VgvD_UunF6-ZqZo49lQnm5gGZM";
                ServiceStackClient.BearerToken = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImtpZCI6ImlwNCJ9.eyJpc3MiOiJzc2p3dCIsInN1YiI6ImViZGJsbHoyM25rcWQ2MjAxODAyMjAxMjAwMzA6YmluaXZhcmdoZXNlQGdtYWlsLmNvbTpkYyIsImlhdCI6MTUzNjI5NTU5NywiZXhwIjoxNTM2Mjk1Njg3LCJlbWFpbCI6ImJpbml2YXJnaGVzZUBnbWFpbC5jb20iLCJjaWQiOiJlYmRibGx6MjNua3FkNjIwMTgwMjIwMTIwMDMwIiwidWlkIjo1LCJ3YyI6ImRjIn0.SjrV2ebJFbVTPHvNbUWteR6nZ42uoRtbx84QGMAqpbu_F9pmVx5AI23f - yFYbtCwNi1o8h3NfV_5_Ymu9siFzFsj3xDsDfiTmNBZZYjO2l11B - h36Z6GWZAkZPjcOF84va3AOnaPxsu3Yq - jWQilU0Sm4__p0AW0lex1dzZOBa0";

                MailMessage mm = new MailMessage("expressbasesystems@gmail.com", request.To)
                {
                    Subject = request.Subject,
                    IsBodyHtml = true,
                    Body = request.Message,

                };
                mm.Attachments.Add(new Attachment(request.AttachmentReport.Memorystream, request.AttachmentName + ".pdf"));
                mm.CC.Add(request.Cc);
                mm.Bcc.Add(request.Bcc);
                System.Net.Mail.SmtpClient smtp = new System.Net.Mail.SmtpClient
                {
                    Host = "smtp.gmail.com",
                    Port = 587,
                    EnableSsl = true,
                    Credentials = new NetworkCredential { UserName = "expressbasesystems@gmail.com", Password = "ebsystems" }

                };
                smtp.Send(mm);
            }
            catch (Exception e)
            {
                return e.Message;
            }
            return null;
        }
    }
}
