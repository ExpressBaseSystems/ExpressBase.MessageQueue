using ExpressBase.Common;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.EbServiceStack.ReqNRes;
using ExpressBase.Common.Enums;
using ExpressBase.Common.ServerEvents_Artifacts;
using ExpressBase.Common.ServiceClients;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.Services;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Data.Common;
using System.Linq;
using System.Net.Http;

namespace ExpressBase.MessageQueue.MQServices
{
    [Restrict(InternalOnly = true)]
    public class FileServiceInternal : EbMqBaseService
    {
        public FileServiceInternal(IMessageProducer _mqp, IMessageQueueClient _mqc, IEbServerEventClient _sec) : base(_mqp, _mqc, _sec)
        {
            MqResponse = new EbMqResponse()
            {
                ReqType = this.GetType().ToString()
            };
        }

        public EbMqResponse Post(UploadFileRequest request)
        {
            Log.Info("Inside Upload Img MQ Service");

            try
            {
                EbConnectionFactory _ebConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);

                string filestore_sid = _ebConnectionFactory.FilesDB.UploadFile(
                    request.FileRefId.ToString(),
                    request.Byte,
                    request.FileCategory,
                    request.InfraConID
                    );

                string sql = EbConnectionFactory.DataDB.Eb_MQ_UPLOADFILE;

                DbParameter[] parameters =
                {
                        _ebConnectionFactory.DataDB.GetNewParameter("filestoresid",EbDbTypes.String, filestore_sid),
                        _ebConnectionFactory.DataDB.GetNewParameter("refid",EbDbTypes.Int32, request.FileRefId),

                        _ebConnectionFactory.DataDB.GetNewParameter("length",EbDbTypes.Int64, request.Byte.Length),

                        _ebConnectionFactory.DataDB.GetNewParameter("filedb_con_id", EbDbTypes.Int32, _ebConnectionFactory.FilesDB.UsedConId),

                        _ebConnectionFactory.DataDB.GetNewParameter("is_image",EbDbTypes.Boolean, 'F')
                };

                var iCount = _ebConnectionFactory.DataDB.DoQuery(sql, parameters);

                if (iCount.Rows.Capacity > 0)
                {
                    this.ServerEventClient.BearerToken = request.BToken;
                    this.ServerEventClient.RefreshToken = request.RToken;
                    this.ServerEventClient.RefreshTokenUri = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_GET_ACCESS_TOKEN_URL);
                    this.ServerEventClient.Post<NotifyResponse>(new NotifyUserIdRequest
                    {
                        Msg = request.FileRefId,
                        Selector = StaticFileConstants.UPLOADSUCCESS,
                        ToUserAuthId = request.UserAuthId,
                    });
                }
            }
            catch (Exception e)
            {
                Log.Error("UploadFile:" + e.ToString());
                MqResponse.IsError = true;
                MqResponse.ErrorString = e.ToString();
            }
            return MqResponse;
        }

        public EbMqResponse Post(UploadImageRequest request)
        {
            EbDataTable iCountOrg = new EbDataTable();
            try
            {
                Log.Info("Start");
                Console.ForegroundColor = ConsoleColor.DarkYellow;
                Log.Info("Log 1");
                Log.Info("\n Req Object : " + request.BToken + "\n solId : " + request.SolnId);
                Log.Info("---ServerEventClient1 BaseUri: " + this.ServerEventClient.BaseUri);
                Log.Info("---ServerEventClient BToken: " + this.ServerEventClient.BearerToken);
                this.ServerEventClient.BearerToken = request.BToken;
                this.ServerEventClient.RefreshToken = request.RToken;
                this.ServerEventClient.RefreshTokenUri = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_GET_ACCESS_TOKEN_URL);
                Log.Info("---ServerEventClient2 BaseUri: " + this.ServerEventClient.BaseUri);
                Log.Info("---ServerEventClient BToken: " + this.ServerEventClient.BearerToken);
                Console.ForegroundColor = ConsoleColor.White;
                this.EbConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);
                Log.Info("Log 2");
                if (this.EbConnectionFactory.ImageManipulate != null && request.Byte.Length > 307200)
                {
                    Log.Info("Log 3");
                    try
                    {
                        int qlty = (int)(51200000 / request.Byte.Length);  //Avg size*100 to get the const int (this case 500kb * 100%)

                        qlty = qlty < 15 ? 15 : qlty;

                        string Clodinaryurl = this.EbConnectionFactory.ImageManipulate[0].Resize
                                                            (request.Byte, request.ImageRefId.ToString(), qlty);

                        Log.Info("Log 3.25");
                        if (!string.IsNullOrEmpty(Clodinaryurl))
                        {
                            using (var client = new HttpClient())
                            {
                                var response = client.GetAsync(Clodinaryurl).Result;

                                if (response.IsSuccessStatusCode)
                                {
                                    var responseContent = response.Content;

                                    request.ImgManpSerConId = this.EbConnectionFactory.ImageManipulate[0].InfraConId;
                                    request.Byte = responseContent.ReadAsByteArrayAsync().Result;
                                }
                                Log.Info("Log 3.5");
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Log.Error("UploadImage Inside: " + e.ToString());
                    }
                }

                Log.Info("Log 4");
                Log.Info("FilesDb: " + this.EbConnectionFactory.FilesDB.DefaultConId);
                string filestore_sid = "";

                try
                {
                    Log.Info("FilesDB Collection Count: " + this.EbConnectionFactory.FilesDB.Count);
                    filestore_sid = this.EbConnectionFactory.FilesDB.UploadFile(request.ImageRefId.ToString(), request.Byte, request.FileCategory, request.InfraConID);
                }
                catch (Exception e)
                {
                    Log.Error("Upload Image Error" + e.StackTrace);
                }
                Log.Info("FilesDb: " + this.EbConnectionFactory.FilesDB.UsedConId);
                Log.Info("File StoreId: " + filestore_sid);

                DbParameter[] parameters =
                {
                        this.EbConnectionFactory.DataDB.GetNewParameter("refid", EbDbTypes.Int32, request.ImageRefId),
                        this.EbConnectionFactory.DataDB.GetNewParameter("filestoreid", EbDbTypes.String, filestore_sid),

                        this.EbConnectionFactory.DataDB.GetNewParameter("length", EbDbTypes.Int64, request.Byte.Length),
                        this.EbConnectionFactory.DataDB.GetNewParameter("imagequality_id", EbDbTypes.Int32, (int)request.ImgQuality),

                        this.EbConnectionFactory.DataDB.GetNewParameter("filedb_con_id", EbDbTypes.Int32,EbConnectionFactory.FilesDB.UsedConId),
                        this.EbConnectionFactory.DataDB.GetNewParameter("imgmanpserid", EbDbTypes.Int32, request.ImgManpSerConId),

                        this.EbConnectionFactory.DataDB.GetNewParameter("is_image", EbDbTypes.Boolean, 'T')
                };

                Log.Info("Log 4.5");

                iCountOrg = this.EbConnectionFactory.DataDB.DoQuery(EbConnectionFactory.DataDB.EB_IMGREFUPDATESQL, parameters);

                Log.Info("Log 5");

                if (iCountOrg.Rows.Capacity > 0)
                {
                    this.ServerEventClient.Post<NotifyResponse>(new NotifyUserIdRequest
                    {
                        Msg = request.ImageRefId,
                        Selector = StaticFileConstants.UPLOADSUCCESS,
                        ToUserAuthId = request.UserAuthId,
                    });
                    Log.Info("Log 6");

                    if (this.EbConnectionFactory.ImageManipulate != null && this.EbConnectionFactory.ImageManipulate[0].InfraConId != 0)
                    {
                        string thumbUrl = this.EbConnectionFactory.ImageManipulate[0].GetImgSize(request.Byte, request.ImageRefId.ToString(), ImageQuality.small);

                        //TO Get thumbnail
                        if (!string.IsNullOrEmpty(thumbUrl))
                        {
                            byte[] thumbnailBytes;

                            Log.Info("UploadImage: ThumbUrl: " + thumbUrl);

                            using (var client = new HttpClient())
                            {
                                var response = client.GetAsync(thumbUrl).Result;

                                if (response.IsSuccessStatusCode)
                                {
                                    var responseContent = response.Content;

                                    // by calling .Result you are synchronously reading the result
                                    thumbnailBytes = responseContent.ReadAsByteArrayAsync().Result;

                                    Log.Info("Log 7");


                                    if (thumbnailBytes.Length > 0)
                                    {
                                        filestore_sid = this.EbConnectionFactory.FilesDB.UploadFile(request.ImageRefId.ToString(), thumbnailBytes, request.FileCategory, request.InfraConID);
                                        DbParameter[] parametersImageSmall =
                                                        {
                                                        this.EbConnectionFactory.DataDB.GetNewParameter("refid", EbDbTypes.Int32, request.ImageRefId),
                                                        this.EbConnectionFactory.DataDB.GetNewParameter("filestoreid", EbDbTypes.String, filestore_sid),

                                                        this.EbConnectionFactory.DataDB.GetNewParameter("length", EbDbTypes.Int64, thumbnailBytes.Length),
                                                        this.EbConnectionFactory.DataDB.GetNewParameter("imagequality_id", EbDbTypes.Int32, (int)ImageQuality.small),

                                                        this.EbConnectionFactory.DataDB.GetNewParameter("filedb_con_id", EbDbTypes.Int32,this.EbConnectionFactory.FilesDB.UsedConId),
                                                        this.EbConnectionFactory.DataDB.GetNewParameter("imgmanpserid", EbDbTypes.Int32, this.EbConnectionFactory.ImageManipulate[0].InfraConId),

                                                        this.EbConnectionFactory.DataDB.GetNewParameter("is_image", EbDbTypes.Boolean, 'T')
                                                };

                                        Log.Info("Log 8");

                                        var iCountSmall = this.EbConnectionFactory.DataDB.DoQuery(EbConnectionFactory.DataDB.EB_IMGREFUPDATESQL, parametersImageSmall);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error("UploadImage Outside:" + e.StackTrace + "\n" + e.SerializeToString());
                if (iCountOrg.Rows.Capacity == 0)
                    this.ServerEventClient.Post<NotifyResponse>(new NotifyUserIdRequest
                    {
                        Msg = request.ImageRefId,
                        Selector = StaticFileConstants.UPLOADFAILURE,
                        ToUserAuthId = request.UserAuthId,
                    });
                Log.Info("Log 9");
                MqResponse.IsError = true;
                MqResponse.ErrorString = e.ToString();
            }
            return MqResponse;
        }

        public EbMqResponse Post(UploadDpRequest request)
        {
            this.ServerEventClient.BearerToken = request.BToken;
            this.ServerEventClient.RefreshToken = request.RToken;
            this.ServerEventClient.RefreshTokenUri = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_GET_ACCESS_TOKEN_URL);

            try
            {
                this.EbConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);

                string filestore_sid = this.EbConnectionFactory.FilesDB.UploadFile(request.ImageRefId.ToString(), request.Byte, request.FileCategory, request.InfraConID);

                DbParameter[] parameters =
                {
                        this.EbConnectionFactory.DataDB.GetNewParameter("refid", EbDbTypes.Int32, request.ImageRefId),
                        this.EbConnectionFactory.DataDB.GetNewParameter("filestoreid", EbDbTypes.String, filestore_sid),
                        this.EbConnectionFactory.DataDB.GetNewParameter("length", EbDbTypes.Int64, request.Byte.Length),
                        this.EbConnectionFactory.DataDB.GetNewParameter("imagequality_id", EbDbTypes.Int32, ImageQuality.original),
                        this.EbConnectionFactory.DataDB.GetNewParameter("filedb_con_id", EbDbTypes.Int32, this.EbConnectionFactory.FilesDB.UsedConId),
                        this.EbConnectionFactory.DataDB.GetNewParameter("imgmanpserid", EbDbTypes.Int32, 0),
                        this.EbConnectionFactory.DataDB.GetNewParameter("is_image", EbDbTypes.Boolean, true),
                        this.EbConnectionFactory.DataDB.GetNewParameter("userid", EbDbTypes.Int32, request.UserId)
                };

                var iCount = this.EbConnectionFactory.DataDB.DoQuery(EbConnectionFactory.DataDB.EB_DPUPDATESQL, parameters);

                if (iCount.Rows.Capacity > 0)
                {
                    this.ServerEventClient.Post<NotifyResponse>(new NotifyUserIdRequest
                    {
                        Msg = request.ImageRefId,
                        Selector = StaticFileConstants.UPLOADSUCCESS,
                        ToUserAuthId = request.UserAuthId,
                    });
                }

                if (this.EbConnectionFactory.ImageManipulate != null)
                {
                    string Clodinaryurl = this.EbConnectionFactory.ImageManipulate[0].GetImgSize
                                                        (request.Byte, request.ImageRefId.ToString(), ImageQuality.small);


                    if (!string.IsNullOrEmpty(Clodinaryurl))
                    {
                        using (var client = new HttpClient())
                        {
                            var response = client.GetAsync(Clodinaryurl).Result;

                            if (response.IsSuccessStatusCode)
                            {
                                var responseContent = response.Content;

                                request.ImgManpSerConId = this.EbConnectionFactory.ImageManipulate[0].InfraConId;
                                byte[] smallByte = responseContent.ReadAsByteArrayAsync().Result;
                                string fStoreIdSmall = this.EbConnectionFactory.FilesDB.UploadFile(request.ImageRefId.ToString(), smallByte, request.FileCategory, request.InfraConID);

                                DbParameter[] smallImgParams =
                                {
                                                        this.EbConnectionFactory.DataDB.GetNewParameter("refid", EbDbTypes.Int32, request.ImageRefId),
                                                        this.EbConnectionFactory.DataDB.GetNewParameter("filestoreid", EbDbTypes.String, fStoreIdSmall),
                                                        this.EbConnectionFactory.DataDB.GetNewParameter("length", EbDbTypes.Int64, request.Byte.Length),
                                                        this.EbConnectionFactory.DataDB.GetNewParameter("imagequality_id", EbDbTypes.Int32, ImageQuality.small),
                                                        this.EbConnectionFactory.DataDB.GetNewParameter("filedb_con_id", EbDbTypes.Int32,this.EbConnectionFactory.FilesDB.UsedConId),
                                                        this.EbConnectionFactory.DataDB.GetNewParameter("imgmanpserid", EbDbTypes.Int32, request.ImgManpSerConId),
                                                        this.EbConnectionFactory.DataDB.GetNewParameter("is_image", EbDbTypes.Boolean, 'T'),
                                                        this.EbConnectionFactory.DataDB.GetNewParameter("userid", EbDbTypes.Int32, request.UserId)
                                };

                                var iSmallCount = this.EbConnectionFactory.DataDB.DoQuery(EbConnectionFactory.DataDB.EB_DPUPDATESQL, smallImgParams);
                            }
                        }
                    }
                }

            }
            catch (Exception e)
            {
                if (request.ImgQuality == ImageQuality.original)
                {
                    this.ServerEventClient.Post<NotifyResponse>(new NotifyUserIdRequest
                    {
                        Msg = request.ImageRefId,
                        Selector = StaticFileConstants.UPLOADFAILURE,
                        ToUserAuthId = request.UserAuthId,
                    });
                }
                Log.Error("UploadImage:" + e.ToString());
                MqResponse.IsError = true;
                MqResponse.ErrorString = e.ToString();
            }
            return MqResponse;
        }

        public EbMqResponse Post(UploadLogoRequest request)
        {
            this.ServerEventClient.BearerToken = request.BToken;
            this.ServerEventClient.RefreshToken = request.RToken;
            this.ServerEventClient.RefreshTokenUri = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_GET_ACCESS_TOKEN_URL);

            try
            {
                //if (this.EbConnectionFactory.ImageManipulate != null && request.Byte.Length > 307200)
                //{
                //    int qlty = (int)(20480000 / request.Byte.Length);

                //    qlty = qlty < 7 ? 7 : qlty;

                //    string Clodinaryurl = this.EbConnectionFactory.ImageManipulate.Resize
                //                                        (request.Byte, request.ImageRefId.ToString(), qlty);

                //    if (!string.IsNullOrEmpty(Clodinaryurl))
                //    {
                //        using (var client = new HttpClient())
                //        {
                //            var response = client.GetAsync(Clodinaryurl).Result;

                //            if (response.IsSuccessStatusCode)
                //            {
                //                var responseContent = response.Content;

                //                request.ImgManpSerConId = this.EbConnectionFactory.ImageManipulate.InfraConId;
                //                request.Byte = responseContent.ReadAsByteArrayAsync().Result;
                //            }
                //        }
                //    }
                //}

                string filestore_sid = this.InfraConnectionFactory.FilesDB.UploadFile(request.ImageRefId.ToString(), request.Byte, request.FileCategory, request.InfraConID);


                DbParameter[] parameters =
                {
                        this.InfraConnectionFactory.DataDB.GetNewParameter("refid", EbDbTypes.Int32, request.ImageRefId),
                        this.InfraConnectionFactory.DataDB.GetNewParameter("filestoreid", EbDbTypes.String, filestore_sid),
                        this.InfraConnectionFactory.DataDB.GetNewParameter("length", EbDbTypes.Int64, request.Byte.Length),
                        this.InfraConnectionFactory.DataDB.GetNewParameter("imagequality_id", EbDbTypes.Int32, (int)request.ImgQuality),
                        this.InfraConnectionFactory.DataDB.GetNewParameter("filedb_con_id", EbDbTypes.Int32, request.InfraConID),
                        this.InfraConnectionFactory.DataDB.GetNewParameter("imgmanpserid", EbDbTypes.Int32, request.ImgManpSerConId),
                        this.InfraConnectionFactory.DataDB.GetNewParameter("is_image", EbDbTypes.Boolean, true),
                        this.InfraConnectionFactory.DataDB.GetNewParameter("solnid", EbDbTypes.String, request.SolutionId)
                };

                var iCount = this.InfraConnectionFactory.DataDB.DoQuery(EbConnectionFactory.DataDB.EB_LOGOUPDATESQL, parameters);

                if (iCount.Rows.Capacity > 0)
                {
                    this.ServerEventClient.Post<NotifyResponse>(new NotifyUserIdRequest
                    {
                        Msg = request.ImageRefId,
                        Selector = StaticFileConstants.UPLOADSUCCESS,
                        ToUserAuthId = request.UserAuthId,
                    });
                }
            }
            catch (Exception e)
            {
                if (request.ImgQuality == ImageQuality.original)
                {
                    this.ServerEventClient.Post<NotifyResponse>(new NotifyUserIdRequest
                    {
                        Msg = request.ImageRefId,
                        Selector = StaticFileConstants.UPLOADFAILURE,
                        ToUserAuthId = request.UserAuthId,
                    });
                }
                Log.Error("UploadImage:" + e.ToString());
                MqResponse.IsError = true;
                MqResponse.ErrorString = e.ToString();
            }
            return MqResponse;
        }
    }

    [Restrict(InternalOnly = true)]
    public class CloudinaryInternal : EbMqBaseService
    {
        public CloudinaryInternal(IMessageProducer _mqp, IMessageQueueClient _mqc) : base(_mqp, _mqc) { }

        //public EbMqResponse Post(GetImageFtpRequest request)
        //{
        //    try
        //    {
        //        this.EbConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);

        //        long size = this.EbConnectionFactory.FTP.GetFileSize(request.FileUrl.Value);


        //        if (size > 0)
        //        {
        //            int id = FileExists(this.EbConnectionFactory.DataDB, fname: request.FileUrl.Value.SplitOnLast('/').Last(), CustomerId: request.FileUrl.Key, IsExist: 1);
        //            if (id > 0)
        //                Log.Info("Counter Updated (File Exists)");
        //            else
        //                Log.Info("Counter Not Updated (File Exists)");


        //            byte[] _byte = this.EbConnectionFactory.FTP.Download(request.FileUrl.Value);

        //            if (_byte.Length > 0)
        //            {
        //                if (UpdateCounter(this.EbConnectionFactory.DataDB, id: id, IsFtp: 1))
        //                    Log.Info("Counter Updated (Byte Received)");
        //                else
        //                    Log.Info("Counter Not Updated (Byte Received)");


        //                UploadImageRequest ImageReq = new UploadImageRequest()
        //                {
        //                    ImgQuality = ImageQuality.original,
        //                    FileCategory = EbFileCategory.Images,

        //                    Byte = _byte,

        //                    UserId = request.UserId,
        //                    SolnId = request.SolnId,

        //                    BToken = request.BToken,
        //                    RToken = request.RToken
        //                };

        //                ImageReq.ImageRefId = GetFileRefId(this.EbConnectionFactory.DataDB, request.UserId, request.FileUrl.Value.Split('/').Last(), request.FileUrl.Value.Split('.').Last(), String.Format(@"CustomerId: {0}", request.FileUrl.Key.ToString()), EbFileCategory.Images);

        //                Console.WriteLine(@"File Recieved) ");

        //                object _imgenum = null;

        //                bool isImage = (Enum.TryParse(typeof(ImageTypes), request.FileUrl.Value.Split('.').Last().ToLower(), out _imgenum));

        //                if (MapFilesWithUser(this.EbConnectionFactory, request.FileUrl.Key, ImageReq.ImageRefId) < 1)
        //                    throw new Exception("File Mapping Failed");

        //                if (isImage)
        //                {
        //                    byte[] ThumbnailBytes;

        //                    if (UpdateCounter(this.EbConnectionFactory.DataDB, id: id, IsImg: 1))
        //                        Log.Info("Counter Updated (IsImage)");

        //                    if (ImageReq.Byte.Length > 307200)
        //                    {
        //                        int qlty = (int)(20480000 / ImageReq.Byte.Length);

        //                        qlty = qlty < 7 ? 7 : qlty;

        //                        Log.Info("Need to Compress");
        //                        string Clodinaryurl = this.EbConnectionFactory.ImageManipulate[0].Resize
        //                                            (ImageReq.Byte, ImageReq.ImageRefId.ToString(), qlty);

        //                        ImageReq.ImgManpSerConId = this.EbConnectionFactory.ImageManipulate[0].InfraConId;

        //                        using (var client = new HttpClient())
        //                        {
        //                            var response = client.GetAsync(Clodinaryurl).Result;

        //                            if (response.IsSuccessStatusCode)
        //                            {
        //                                var responseContent = response.Content;

        //                                // by calling .Result you are synchronously reading the result
        //                                ImageReq.Byte = responseContent.ReadAsByteArrayAsync().Result;
        //                                if (UpdateCounter(this.EbConnectionFactory.DataDB, id: id, IsCloudLarge: 1))
        //                                    Log.Info("Counter Updated (Recieved Large Image)");
        //                            }
        //                            else
        //                            {
        //                                throw new Exception("Cloudinary Error: Image Not Downloaded");
        //                            }
        //                        }

        //                    }

        //                    string thumbUrl = this.EbConnectionFactory.ImageManipulate[0].GetImgSize(ImageReq.Byte, request.FileUrl.Value.Split('/').Last(), ImageQuality.small);


        //                    this.MessageProducer3.Publish(ImageReq);
        //                    Log.Info("Pushed Image Large to Queue");


        //                    //TO Get thumbnail
        //                    using (var client = new HttpClient())
        //                    {
        //                        var response = client.GetAsync(thumbUrl).Result;

        //                        if (response.IsSuccessStatusCode)
        //                        {
        //                            var responseContent = response.Content;

        //                            // by calling .Result you are synchronously reading the result
        //                            ThumbnailBytes = responseContent.ReadAsByteArrayAsync().Result;
        //                        }
        //                        else
        //                        {
        //                            throw new Exception("Cloudinary Error: Transformed Image Not Available");
        //                        }

        //                    }


        //                    if (ThumbnailBytes.Length > 0)
        //                    {
        //                        ImageReq.Byte = ThumbnailBytes;
        //                        ImageReq.ImgManpSerConId = this.EbConnectionFactory.ImageManipulate[0].InfraConId;
        //                        ImageReq.ImgQuality = ImageQuality.small;

        //                        this.MessageProducer3.Publish(ImageReq);

        //                        if (UpdateCounter(this.EbConnectionFactory.DataDB, id: id, IsCloudSmall: 1))
        //                            Log.Info("Counter Updated (Recieved SmallImage)");
        //                        Log.Info("Pushed Small to Queue after Cloudinary");
        //                    }


        //                }
        //                else
        //                {
        //                    this.MessageProducer3.Publish(new UploadFileRequest()
        //                    {
        //                        FileRefId = ImageReq.ImageRefId,
        //                        FileCategory = EbFileCategory.File,

        //                        Byte = ImageReq.Byte,

        //                        UserId = request.UserId,
        //                        SolnId = request.SolnId,

        //                        BToken = request.BToken,
        //                        RToken = request.RToken
        //                    });
        //                    if (UpdateCounter(this.EbConnectionFactory.DataDB, id: id, IsFile: 1))
        //                        Log.Info("Counter Updated (File Pushed)");
        //                }
        //            }
        //        }
        //    }
        //    catch (Exception e)
        //    {
        //        Log.Error("MQ Exception: " + e.ToString());
        //        return new EbMqResponse();
        //    }
        //    return new EbMqResponse { IsError = false };
        //}

        //private int MapFilesWithUser(EbConnectionFactory connectionFactory, int CustomerId, int FileRefId)
        //{
        //    int res = 0;
        //    string MapQuery = @"INSERT into customer_files (customer_id, eb_files_ref_id) values(@cust_id, @ref_id) returning id";
        //    DbParameter[] MapParams =
        //    {
        //                connectionFactory.DataDB.GetNewParameter("cust_id", EbDbTypes.Int32, CustomerId),
        //                connectionFactory.DataDB.GetNewParameter("ref_id", EbDbTypes.Int32, FileRefId)
        //    };
        //    var table = connectionFactory.DataDB.DoQuery(MapQuery, MapParams);
        //    res = (int)table.Rows[0][0];
        //    return res;
        //}

        //public bool UpdateCounter(IDatabase DataDB, int id, int IsFtp = 0, int IsCloudLarge = 0, int IsCloudSmall = 0, int IsFile = 0, int IsImg = 0)
        //{
        //    int res = 0;

        //    try
        //    {
        //        string MapQuery = @"
        //                            UPDATE 
        //                                eb_image_migration_counter
        //                            SET 
	       //                             ftp_get = eb_image_migration_counter.ftp_get + @ftp, 
	       //                             cldnry_large = eb_image_migration_counter.cldnry_large + @cldl , 
	       //                             cldnry_small = eb_image_migration_counter.cldnry_small + @clds, 
	       //                             file_upld = eb_image_migration_counter.file_upld + @file, 
	       //                             img_org = eb_image_migration_counter.img_org + @img
        //                            WHERE 
        //                                eb_image_migration_counter.id = @id;";
        //        DbParameter[] MapParams =
        //        {
        //                        DataDB.GetNewParameter("id", EbDbTypes.Int32, id),
        //                        DataDB.GetNewParameter("ftp", EbDbTypes.Int32, IsFtp),
        //                        DataDB.GetNewParameter("cldl", EbDbTypes.Int32, IsCloudLarge),
        //                        DataDB.GetNewParameter("clds", EbDbTypes.Int32, IsCloudSmall),
        //                        DataDB.GetNewParameter("file", EbDbTypes.Int32, IsFile),
        //                        DataDB.GetNewParameter("img", EbDbTypes.Int32, IsImg)
        //            };
        //        res = DataDB.DoNonQuery(MapQuery, MapParams);
        //    }
        //    catch (Exception e)
        //    {
        //        Log.Error("Counter: " + e.Message);
        //    }
        //    return (res > 0);
        //}

        //public int FileExists(IDatabase DataDB, string fname, int CustomerId, int IsExist = 0)
        //{
        //    int res = 0;

        //    try
        //    {
        //        string AddQuery = EbConnectionFactory.DataDB.EB_FILEEXISTS;

        //        DbParameter[] MapParams =
        //        {
        //                        DataDB.GetNewParameter("cid", EbDbTypes.Int32, CustomerId),
        //                        DataDB.GetNewParameter("fname", EbDbTypes.String, fname),
        //                        DataDB.GetNewParameter("exist", EbDbTypes.Int32, IsExist),
        //            };
        //        var tab = DataDB.DoQuery(AddQuery, MapParams);
        //        if (tab.Rows.Capacity > 0)
        //            res = Convert.ToInt32(tab.Rows[0][0]);
        //    }
        //    catch (Exception e)
        //    {
        //        Log.Error("Counter: " + e.Message);
        //    }
        //    return res;
        //}

        private int GetFileRefId(IDatabase datadb, int userId, string filename, string filetype, string tags, EbFileCategory ebFileCategory)
        {
            try
            {
                string IdFetchQuery = EbConnectionFactory.DataDB.EB_GETFILEREFID;

                DbParameter[] parameters =
                   {
                        datadb.GetNewParameter("userid", EbDbTypes.Int32, userId),
                        datadb.GetNewParameter("filename", EbDbTypes.String, filename),
                        datadb.GetNewParameter("filetype", EbDbTypes.String, filetype),
                        datadb.GetNewParameter("tags", EbDbTypes.String, tags),
                        datadb.GetNewParameter("filecategory", EbDbTypes.Int16, ebFileCategory)
            };
                var table = datadb.DoQuery(IdFetchQuery, parameters);

                return (int)table.Rows[0][0];
            }
            catch (Exception e)
            {
                return 0;
            }
        }
    }
}
