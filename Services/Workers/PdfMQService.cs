using ExpressBase.Common;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.ServerEvents_Artifacts;
using ExpressBase.Common.ServiceClients;
using ExpressBase.Common.Singletons;
using ExpressBase.Objects;
using ExpressBase.Objects.Helpers;
using ExpressBase.Objects.Services;
using ExpressBase.Objects.ServiceStack_Artifacts;
using iTextSharp.text;
using iTextSharp.text.pdf;
using Newtonsoft.Json;
using ServiceStack;
using ServiceStack.Auth;
using ServiceStack.Messaging;
using ServiceStack.Redis;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text.RegularExpressions;
using HeaderFooter = ExpressBase.Objects.Helpers.HeaderFooter;

namespace ExpressBase.MessageQueue.Services.Workers
{
    [Restrict(InternalOnly = true)]
    public class PdfMQService : EbMqBaseService
    {
        public PdfMQService(IEbConnectionFactory _dbf, IEbStaticFileClient _sfc, IMessageProducer _mqp, IMessageQueueClient _mqc, IServiceClient _ssclient, IEbServerEventClient _sec, PooledRedisClientManager pooledRedisManager) : base(_dbf, _sfc, _mqp, _mqc, _ssclient, _sec, pooledRedisManager) { }

        public MemoryStream Ms1 = null;

        public EbReport Report = null;

        public PdfWriter Writer = null;

        public Document MainDocument = null;

        public PdfContentByte Canvas = null;

        public ReportRenderResponse Post(ReportRenderMultipleMQRequest request)
        {
            string Displayname = "";
            this.Ms1 = new MemoryStream();
            this.EbConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);

            JsonServiceClient serviceClient = this.ServiceStackClient;
            MyAuthenticateResponse authResponse = serviceClient.Get<MyAuthenticateResponse>(new Authenticate
            {
                provider = CredentialsAuthProvider.Name,
                UserName = GetUserObject(request.ReadingUserAuthId)?.Email,
                Password = "NIL",
                Meta = new Dictionary<string, string> {
                        { RoutingConstants.WC, RoutingConstants.UC },
                        { TokenConstants.CID, request.SolnId},
                        { "sso", "true" },
                        { TokenConstants.IP, ""},
                        { RoutingConstants.USER_AGENT, ""}
                    },
            });
            this.FileClient.BearerToken = authResponse?.BearerToken;
            this.FileClient.RefreshToken = authResponse?.RefreshToken;

            EbReport reportObject = EbFormHelper.GetEbObject<EbReport>(request.RefId, serviceClient, this.Redis, this);

            Displayname = Regex.Replace(((Displayname == "") ? reportObject.DisplayName : Displayname), @"\s+", "");
            int id = new DownloadsPageHelper().InsertDownloadFileEntry(this.EbConnectionFactory.DataDB, Displayname + ".pdf", request.UserId);

            reportObject.pooledRedisManager = this.PooledRedisManager;
            reportObject.ObjectsDB = this.EbConnectionFactory.ObjectsDB;
            reportObject.Redis = this.Redis;
            reportObject.FileClient = this.FileClient;
            reportObject.Solution = GetSolutionObject(request.SolnId);
            reportObject.ReadingUser = GetUserObject(request.ReadingUserAuthId);
            reportObject.RenderingUser = GetUserObject(request.RenderingUserAuthId);

            reportObject.CultureInfo = CultureHelper.GetSerializedCultureInfo(reportObject.ReadingUser?.Preference.Locale ?? "en-US").GetCultureInfo();
            reportObject.GetWatermarkImages();


            if (request.Params != null)
            {
                try
                {
                    byte[] encodedDataAsBytes = System.Convert.FromBase64String(request.Params);
                    string returnValue = System.Text.ASCIIEncoding.ASCII.GetString(encodedDataAsBytes);

                    List<Param> _paramlist = (returnValue == null) ? null : JsonConvert.DeserializeObject<List<Param>>(returnValue);
                    if (_paramlist != null)
                    {
                        for (int i = 0; i < _paramlist.Count; i++)
                        {
                            string[] values = _paramlist[i].Value.Split(',');
                            reportObject.CurrentReportPageNumber = 1;

                            for (int j = 0; j < values.Length; j++)
                            {
                                List<Param> _newParamlist = new List<Param> { new Param { Name = "id", Value = values[j], Type = "7" } };

                                this.Report = reportObject;

                                if (Report == null) continue;

                                Report.Reset();

                                Report.GetData4Pdf(_newParamlist, EbConnectionFactory);

                                if (j > 0)
                                {
                                    reportObject.NextReport = true;
                                    Report.AddNewPage();
                                }
                                InitializePdfObjects();

                                if (!MainDocument.IsOpen())
                                    MainDocument.Open();

                                if (Report.DataSet != null)
                                    Report.Draw();
                                else
                                    throw new Exception();
                                Report.Reset();

                            }

                        }
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("Exception-reportService " + e.Message + e.StackTrace);
                    Report.HandleExceptionPdf(e);
                }

                Report.IsRenderingComplete = true;
                Report.Doc.AddTitle(Report.DocumentName);
                Report.Doc.Close();
                Report.Writer.Close();

                Ms1.Position = 0;

                string uid = request.RefId + request.UserId + request.SubscriptionId;
                byte[] compressedData = Compress(Ms1.ToArray());

                //this.Redis.Set("PdfReport" + uid, compressedData, DateTime.Now.AddMinutes(15));
                new DownloadsPageHelper().SaveDownloadFileBytea(this.EbConnectionFactory.DataDB, compressedData, id);


                this.ServerEventClient.BearerToken = authResponse?.BearerToken;
                this.ServerEventClient.RefreshToken = authResponse?.RefreshToken;
                this.ServerEventClient.RefreshTokenUri = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_GET_ACCESS_TOKEN_URL);

                Console.WriteLine("Calling NotifySubscriptionRequest to subsc.id :" + request.SubscriptionId);
                //this.ServerEventClient.Post<NotifyResponse>(new NotifySubscriptionRequest
                //{
                //    Msg = "/DV/GetPdf?id=" + id,
                //    Selector = StaticFileConstants.PDFDOWNLOADSUCCESS,
                //    ToSubscriptionId = request.SubscriptionId
                //});
            }
            return new ReportRenderResponse();
        }
       
         public void InitializePdfObjects()
        {
            if (this.MainDocument == null)
            {
                float _width = Report.WidthPt - Report.Margin.Left;
                float _height = Report.HeightPt - Report.Margin.Top - Report.Margin.Bottom;
                Report.HeightPt = _height;

                Rectangle rec = new Rectangle(_width, _height);
                Report.Doc = new Document(rec);
                Report.Doc.SetMargins(Report.Margin.Left, Report.Margin.Right, Report.Margin.Top, Report.Margin.Bottom);
                Report.Writer = PdfWriter.GetInstance(Report.Doc, this.Ms1);
                Report.Writer.PageEvent = new HeaderFooter(Report);
                Report.Writer.Open();
                Report.Writer.CloseStream = true;//important
                Report.Canvas = Report.Writer.DirectContent;
                Report.MasterPageNumber = Report.Writer.PageNumber;
                this.MainDocument = Report.Doc;
                this.Writer = Report.Writer;
                this.Canvas = Report.Canvas;
            }
            else
            {
                Report.Doc = this.MainDocument;
                Report.Writer = this.Writer;
                Report.Canvas = this.Canvas; 
                Report.SerialNumber = 0;
                Report.DrawDetailCompleted = false;
            }
        }
        public static byte[] Compress(byte[] data)
        {
            using (MemoryStream memory = new MemoryStream())
            {
                using (GZipStream gzip = new GZipStream(memory,
                    CompressionMode.Compress, true))
                {
                    gzip.Write(data, 0, data.Length);
                }
                return memory.ToArray();
            }
        }
    }
}