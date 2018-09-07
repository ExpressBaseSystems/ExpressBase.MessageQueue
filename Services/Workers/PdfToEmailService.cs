using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.ServiceClients;
using ExpressBase.Common.Structures;
using ExpressBase.Objects;
using ExpressBase.Objects.EmailRelated;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.ServiceStack;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.MessageQueue.Services.Workers
{
    [Restrict(InternalOnly = true)]
    public class PdfToEmailInternalService : EbMqBaseService
    {
        public PdfToEmailInternalService( IServiceClient _ssclient) : base(_ssclient) { }

        public void Post(PdfCreateServiceRequest request)
        {
            ServiceStackClient.BearerToken = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImtpZCI6ImlwNCJ9.eyJpc3MiOiJzc2p3dCIsInN1YiI6ImViZGJsbHoyM25rcWQ2MjAxODAyMjAxMjAwMzA6YmluaXZhcmdoZXNlQGdtYWlsLmNvbTpkYyIsImlhdCI6MTUzNjI5NTU5NywiZXhwIjoxNTM2Mjk1Njg3LCJlbWFpbCI6ImJpbml2YXJnaGVzZUBnbWFpbC5jb20iLCJjaWQiOiJlYmRibGx6MjNua3FkNjIwMTgwMjIwMTIwMDMwIiwidWlkIjo1LCJ3YyI6ImRjIn0.SjrV2ebJFbVTPHvNbUWteR6nZ42uoRtbx84QGMAqpbu_F9pmVx5AI23f-yFYbtCwNi1o8h3NfV_5_Ymu9siFzFsj3xDsDfiTmNBZZYjO2l11B-h36Z6GWZAkZPjcOF84va3AOnaPxsu3Yq-jWQilU0Sm4__p0AW0lex1dzZOBa0";
            ServiceStackClient.RefreshToken = "eyJ0eXAiOiJKV1RSIiwiYWxnIjoiUlMyNTYiLCJraWQiOiJpcDQifQ.eyJzdWIiOiJlYmRibGx6MjNua3FkNjIwMTgwMjIwMTIwMDMwOmJpbml2YXJnaGVzZUBnbWFpbC5jb206ZGMiLCJpYXQiOjE1MzYyOTU1OTcsImV4cCI6MTUzNjM4MTk5N30.IJp7gOSrCy7Nimr8W_AnHXFXcNfAKFKY_ntt97Elwd5E-84EL9PhULE2XiF5-64zr1dGyoYOPFlEdkWD0lOPW8g-UpqK3ycrIDBtrDcvxtR-DhV_1aXnnh3H-O5zCCsyDiF8yBH9rDiPtgjt9VgvD_UunF6-ZqZo49lQnm5gGZM";
            
            var res = ServiceStackClient.Get(new EbObjectParticularVersionRequest() { RefId = request.Refid });
            EbEmailTemplate ebEmailTemplate = new EbEmailTemplate();
            foreach (var element in res.Data)
            {
                ebEmailTemplate = EbSerializers.Json_Deserialize(element.Json);
            }

           
            
            List<Param> _param = new List<Param> { new Param {Name="id",Type= ((int)EbDbTypes.Int32).ToString(), Value= "1" } };
            
            var dsresp = ServiceStackClient.Get(new DataSourceDataRequest { Params = _param ,RefId = ebEmailTemplate.DataSourceRefId });
            var ds2 = dsresp.DataSet;
            var myDsres = ServiceStackClient.Get(new EbObjectParticularVersionRequest() { RefId = ebEmailTemplate.DataSourceRefId });
            EbDataSource ebDataSource = new EbDataSource();
            foreach (var element in myDsres.Data)
            {
                ebDataSource = EbSerializers.Json_Deserialize(element.Json);
            }
            DbParameter[] parameters = { EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, 1) }; //change 1 by request.id
            var ds = EbConnectionFactory.ObjectsDB.DoQueries(ebDataSource.Sql, parameters);
            //var pattern = @"\{{(.*?)\}}";
            //var matches = Regex.Matches(ebEmailTemplate.Body, pattern);
            //Dictionary<string, object> dict = new Dictionary<string, object>();
            foreach (var dscol in ebEmailTemplate.DsColumnsCollection)
            {
                string str = dscol.Title.Replace("{{", "").Replace("}}", "");

                foreach (var dt in ds.Tables)
                {
                    string colname = dt.Rows[0][str.Split('.')[1]].ToString();
                    ebEmailTemplate.Body = ebEmailTemplate.Body.Replace(dscol.Title, colname);
                }
            }

            ProtoBufServiceClient pclient = new ProtoBufServiceClient(ServiceStackClient);
            var RepRes = pclient.Get(new ReportRenderRequest { Refid = ebEmailTemplate.AttachmentReportRefID, Fullname = "MQ", Params = null });
            RepRes.StreamWrapper.Memorystream.Position = 0;

            MessageProducer3.Publish(new EmailServicesRequest()
            {
                From = "request.from",
                To = ebEmailTemplate.To,
                Cc = ebEmailTemplate.Cc,
                Bcc = ebEmailTemplate.Bcc,
                Message = ebEmailTemplate.Body,
                Subject = ebEmailTemplate.Subject,
                UserId = request.UserId,
                UserAuthId = request.UserAuthId,
                SolnId = request.SolnId,
                AttachmentReport = RepRes.StreamWrapper,
                AttachmentName=RepRes.ReportName
            });
        }
        

    }
}
