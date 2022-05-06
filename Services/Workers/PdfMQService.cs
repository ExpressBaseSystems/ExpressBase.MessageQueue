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
        public PdfMQService(IEbConnectionFactory _dbf, IEbStaticFileClient _sfc, IMessageProducer _mqp, IMessageQueueClient _mqc, IServiceClient _ssclient, IEbServerEventClient _sec) : base(_dbf, _sfc, _mqp, _mqc, _ssclient, _sec) { }

        public MemoryStream Ms1 = null;

        public EbReport Report = null;

        public PdfWriter Writer = null;

        public Document Document = null;

        public PdfContentByte Canvas = null;

        public ReportRenderResponse Post(ReportRenderMultipleMQRequest request)
        {
            string Displayname = "";
            this.Ms1 = new MemoryStream();
            this.EbConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);

            JsonServiceClient authClient = this.ServiceStackClient;
            MyAuthenticateResponse authResponse = authClient.Get<MyAuthenticateResponse>(new Authenticate
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

            List<EbObjectWrapper> resultlist = EbObjectsHelper.GetParticularVersion(this.EbConnectionFactory.ObjectsDB, request.RefId);
            EbReport ReportObject= EbSerializers.Json_Deserialize<EbReport>(resultlist[0].Json);

            ReportObject.ObjectsDB = this.EbConnectionFactory.ObjectsDB;
            ReportObject.Redis = this.Redis;
            ReportObject.FileClient = this.FileClient;
            ReportObject.Solution = GetSolutionObject(request.SolnId);
            ReportObject.ReadingUser = GetUserObject(request.ReadingUserAuthId);
            ReportObject.RenderingUser = GetUserObject(request.RenderingUserAuthId);

            ReportObject.CultureInfo = CultureHelper.GetSerializedCultureInfo(ReportObject.ReadingUser?.Preference.Locale ?? "en-US").GetCultureInfo();
            ReportObject.GetWatermarkImages();

            try
            {
                byte[] encodedDataAsBytes = System.Convert.FromBase64String(request.Params);
                string returnValue = System.Text.ASCIIEncoding.ASCII.GetString(encodedDataAsBytes);

                List<Param> _paramlist = (returnValue == null) ? null : JsonConvert.DeserializeObject<List<Param>>(returnValue);
                if (_paramlist != null)
                {
                    foreach (Param p in _paramlist)
                    {
                        string[] values = p.Value.Split(',');
                        foreach (string val in values)
                        {
                            List<Param> _newParamlist = new List<Param>
                            {
                                new Param { Name = "id", Value = val, Type = "7" }
                            };

                            this.Report = ReportObject;

                            if (Report != null)
                            { 
                                InitializePdfObjects();
                                Report.Doc.NewPage();
                                Report.GetData4Pdf(_newParamlist, EbConnectionFactory);

                                if (Report.DataSet != null)
                                    Report.Draw();
                                else
                                    throw new Exception();
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception-reportService " + e.Message + e.StackTrace);
                Report.HandleExceptionPdf();
            }

            Report.Doc.Close();

            if (Report.DataSourceRefId != string.Empty && Report.DataSet != null)
            {
                Report.DataSet.Tables.Clear();
                Report.DataSet = null;
            }

            Displayname = Regex.Replace(((Displayname == "") ? Report.DisplayName : Displayname), @"\s+", "");

            Ms1.Position = 0;

            string uid = request.RefId + request.UserId + request.SubscriptionId;
            byte[] compressedData = Compress(Ms1.ToArray());

            this.Redis.Set("PdfReport" + uid, compressedData, DateTime.Now.AddMinutes(15));

            this.ServerEventClient.BearerToken = authResponse?.BearerToken;
            this.ServerEventClient.RefreshToken = authResponse?.RefreshToken;
            this.ServerEventClient.RefreshTokenUri = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_GET_ACCESS_TOKEN_URL);

            Console.WriteLine("Calling NotifySubscriptionRequest to subsc.id :" + request.SubscriptionId);
            this.ServerEventClient.Post<NotifyResponse>(new NotifySubscriptionRequest
            {
                Msg = "/DV/GetPdf?refid=" + uid + "&filename=" + Displayname + ".pdf",
                Selector = StaticFileConstants.PDFDOWNLOADSUCCESS,
                ToSubscriptionId = request.SubscriptionId
            });

            return new ReportRenderResponse();
        }
        public void InitializePdfObjects()
        {
            float _width = Report.WidthPt - Report.Margin.Left;// - Report.Margin.Right;
            float _height = Report.HeightPt - Report.Margin.Top - Report.Margin.Bottom;
            Report.HeightPt = _height;

            Rectangle rec = new Rectangle(_width, _height);
            if (this.Document == null)
            {
                Report.Doc = new Document(rec);
                Report.Doc.SetMargins(Report.Margin.Left, Report.Margin.Right, Report.Margin.Top, Report.Margin.Bottom);
                Report.Writer = PdfWriter.GetInstance(Report.Doc, this.Ms1);
                Report.Writer.Open();
                Report.Doc.Open();
                Report.Doc.AddTitle(Report.DocumentName);
                Report.Writer.PageEvent = new HeaderFooter(Report);
                Report.Writer.CloseStream = true;//important
                Report.Canvas = Report.Writer.DirectContent;
                Report.PageNumber = Report.Writer.PageNumber;
                this.Document = Report.Doc;
                this.Writer = Report.Writer;
                this.Canvas = Report.Canvas;
            }
            else
            {
                Report.Doc = this.Document;
                Report.Writer = this.Writer;
                Report.Canvas = this.Canvas;
                Report.PageNumber = 1/*Report.Writer.PageNumber*/;
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