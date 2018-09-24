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
        public FileServiceInternal(IMessageProducer _mqp, IMessageQueueClient _mqc, IEbServerEventClient _sec) : base(_mqp, _mqc, _sec) { }

        private const string _imgRefUpdateSql = @"
            INSERT INTO
                eb_files_ref_variations 
                (eb_files_ref_id, filestore_sid, length, imagequality_id, is_image, img_manp_ser_con_id, filedb_con_id)
            VALUES 
                (:refid, :filestoreid, :length, :imagequality_id, :is_image, :imgmanpserid, :filedb_con_id) RETURNING id";

        private const string _dpUpdateSql = @"
            INSERT INTO
                eb_files_ref_variations 
                (eb_files_ref_id, filestore_sid, length, imagequality_id, is_image, img_manp_ser_con_id, filedb_con_id)
            VALUES 
                (:refid, :filestoreid, :length, :imagequality_id, :is_image, :imgmanpserid, :filedb_con_id) RETURNING id;
            UPDATE eb_users SET dprefid = :refid WHERE id=:userid";

        public EbMqResponse Post(UploadFileRequest request)
        {
            Log.Info("Inside Upload Img MQ Service");

            try
            {
                EbConnectionFactory _ebConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);

                string filestore_sid = _ebConnectionFactory.FilesDB.UploadFile(
                    request.FileRefId.ToString(),
                    request.Byte,
                    request.FileCategory
                    );

                string sql = @"
INSERT INTO
    eb_files_ref_variations 
    (eb_files_ref_id, filestore_sid, length, is_image, filedb_con_id)
VALUES 
    (:refid, :filestoresid, :length, :is_image, :filedb_con_id) RETURNING id";

                DbParameter[] parameters =
                {
                        _ebConnectionFactory.DataDB.GetNewParameter("filestoresid",EbDbTypes.String, filestore_sid),
                        _ebConnectionFactory.DataDB.GetNewParameter("refid",EbDbTypes.Int32, request.FileRefId),

                        _ebConnectionFactory.DataDB.GetNewParameter("length",EbDbTypes.Int64, request.Byte.Length),

                        _ebConnectionFactory.DataDB.GetNewParameter("filedb_con_id", EbDbTypes.Int32, _ebConnectionFactory.FilesDB.InfraConId),

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
                return new EbMqResponse();
            }
            return new EbMqResponse { Result = true };
        }

        public EbMqResponse Post(UploadImageRequest request)
        {
            this.ServerEventClient.BearerToken = request.BToken;
            this.ServerEventClient.RefreshToken = request.RToken;
            this.ServerEventClient.RefreshTokenUri = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_GET_ACCESS_TOKEN_URL);
            EbDataTable iCountOrg = new EbDataTable();

            try
            {
                this.EbConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);

                if (this.EbConnectionFactory.ImageManipulate != null && request.Byte.Length > 307200)
                {
                    try
                    {
                        int qlty = (int)(20480000 / request.Byte.Length);

                        qlty = qlty < 10 ? 10 : qlty;

                        string Clodinaryurl = this.EbConnectionFactory.ImageManipulate.Resize
                                                            (request.Byte, request.ImageRefId.ToString(), qlty);


                        if (!string.IsNullOrEmpty(Clodinaryurl))
                        {
                            using (var client = new HttpClient())
                            {
                                var response = client.GetAsync(Clodinaryurl).Result;

                                if (response.IsSuccessStatusCode)
                                {
                                    var responseContent = response.Content;

                                    request.ImgManpSerConId = this.EbConnectionFactory.ImageManipulate.InfraConId;
                                    request.Byte = responseContent.ReadAsByteArrayAsync().Result;
                                }
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Log.Error("UploadImage: " + e.ToString());
                    }
                }

                string filestore_sid = this.EbConnectionFactory.FilesDB.UploadFile(request.ImageRefId.ToString(), request.Byte, request.FileCategory);


                DbParameter[] parameters =
                {
                        this.EbConnectionFactory.DataDB.GetNewParameter("refid", EbDbTypes.Int32, request.ImageRefId),
                        this.EbConnectionFactory.DataDB.GetNewParameter("filestoreid", EbDbTypes.String, filestore_sid),

                        this.EbConnectionFactory.DataDB.GetNewParameter("length", EbDbTypes.Int64, request.Byte.Length),
                        this.EbConnectionFactory.DataDB.GetNewParameter("imagequality_id", EbDbTypes.Int32, (int)request.ImgQuality),

                        this.EbConnectionFactory.DataDB.GetNewParameter("filedb_con_id", EbDbTypes.Int32, this.EbConnectionFactory.FilesDB.InfraConId),
                        this.EbConnectionFactory.DataDB.GetNewParameter("imgmanpserid", EbDbTypes.Int32, request.ImgManpSerConId),

                        this.EbConnectionFactory.DataDB.GetNewParameter("is_image", EbDbTypes.Boolean, 'T')
                };

                iCountOrg = this.EbConnectionFactory.DataDB.DoQuery(_imgRefUpdateSql, parameters);

                if (iCountOrg.Rows.Capacity > 0)
                {
                    this.ServerEventClient.Post<NotifyResponse>(new NotifyUserIdRequest
                    {
                        Msg = request.ImageRefId,
                        Selector = StaticFileConstants.UPLOADSUCCESS,
                        ToUserAuthId = request.UserAuthId,
                    });

                    if (this.EbConnectionFactory.ImageManipulate.InfraConId != 0)
                    {
                        string thumbUrl = this.EbConnectionFactory.ImageManipulate.GetImgSize(request.Byte, request.ImageRefId.ToString(), ImageQuality.small);

                        //TO Get thumbnail
                        if (!string.IsNullOrEmpty(thumbUrl))
                        {
                            byte[] thumbnailBytes;

                            Log.Info("UploadImage: ThumbUrl: "+ thumbUrl);

                            using (var client = new HttpClient())
                            {
                                var response = client.GetAsync(thumbUrl).Result;

                                if (response.IsSuccessStatusCode)
                                {
                                    var responseContent = response.Content;

                                    // by calling .Result you are synchronously reading the result
                                    thumbnailBytes = responseContent.ReadAsByteArrayAsync().Result;

                                    if (thumbnailBytes.Length > 0)
                                    {
                                        filestore_sid = this.EbConnectionFactory.FilesDB.UploadFile(request.ImageRefId.ToString(), thumbnailBytes, request.FileCategory);
                                        DbParameter[] parametersImageSmall =
                                                        {
                                                        this.EbConnectionFactory.DataDB.GetNewParameter("refid", EbDbTypes.Int32, request.ImageRefId),
                                                        this.EbConnectionFactory.DataDB.GetNewParameter("filestoreid", EbDbTypes.String, filestore_sid),

                                                        this.EbConnectionFactory.DataDB.GetNewParameter("length", EbDbTypes.Int64, thumbnailBytes.Length),
                                                        this.EbConnectionFactory.DataDB.GetNewParameter("imagequality_id", EbDbTypes.Int32, (int)ImageQuality.small),

                                                        this.EbConnectionFactory.DataDB.GetNewParameter("filedb_con_id", EbDbTypes.Int32, this.EbConnectionFactory.FilesDB.InfraConId),
                                                        this.EbConnectionFactory.DataDB.GetNewParameter("imgmanpserid", EbDbTypes.Int32, this.EbConnectionFactory.ImageManipulate.InfraConId),

                                                        this.EbConnectionFactory.DataDB.GetNewParameter("is_image", EbDbTypes.Boolean, 'T')
                                                };

                                        var iCountSmall = this.EbConnectionFactory.DataDB.DoQuery(_imgRefUpdateSql, parametersImageSmall);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                if (iCountOrg.Rows.Capacity == 0)
                    this.ServerEventClient.Post<NotifyResponse>(new NotifyUserIdRequest
                    {
                        Msg = request.ImageRefId,
                        Selector = StaticFileConstants.UPLOADFAILURE,
                        ToUserAuthId = request.UserAuthId,
                    });

                Log.Error("UploadImage:" + e.ToString());
                return new EbMqResponse();
            }
            return new EbMqResponse { Result = true };
        }

        public EbMqResponse Post(UploadDpRequest request)
        {
            this.ServerEventClient.BearerToken = request.BToken;
            this.ServerEventClient.RefreshToken = request.RToken;
            this.ServerEventClient.RefreshTokenUri = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_GET_ACCESS_TOKEN_URL);

            try
            {
                this.EbConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);


                string filestore_sid = this.EbConnectionFactory.FilesDB.UploadFile(request.ImageRefId.ToString(), request.Byte, request.FileCategory);


                DbParameter[] parameters =
                {
                        this.EbConnectionFactory.DataDB.GetNewParameter("refid", EbDbTypes.Int32, request.ImageRefId),
                        this.EbConnectionFactory.DataDB.GetNewParameter("filestoreid", EbDbTypes.String, filestore_sid),
                        this.EbConnectionFactory.DataDB.GetNewParameter("length", EbDbTypes.Int64, request.Byte.Length),
                        this.EbConnectionFactory.DataDB.GetNewParameter("imagequality_id", EbDbTypes.Int32, ImageQuality.original),
                        this.EbConnectionFactory.DataDB.GetNewParameter("filedb_con_id", EbDbTypes.Int32, this.EbConnectionFactory.FilesDB.InfraConId),
                        this.EbConnectionFactory.DataDB.GetNewParameter("imgmanpserid", EbDbTypes.Int32, 0),
                        this.EbConnectionFactory.DataDB.GetNewParameter("is_image", EbDbTypes.Boolean, 'T'),
                        this.EbConnectionFactory.DataDB.GetNewParameter("userid", EbDbTypes.Int32, request.UserId)
                };

                var iCount = this.EbConnectionFactory.DataDB.DoQuery(_dpUpdateSql, parameters);

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
                    string Clodinaryurl = this.EbConnectionFactory.ImageManipulate.GetImgSize
                                                        (request.Byte, request.ImageRefId.ToString(), ImageQuality.small);


                    if (!string.IsNullOrEmpty(Clodinaryurl))
                    {
                        using (var client = new HttpClient())
                        {
                            var response = client.GetAsync(Clodinaryurl).Result;

                            if (response.IsSuccessStatusCode)
                            {
                                var responseContent = response.Content;

                                request.ImgManpSerConId = this.EbConnectionFactory.ImageManipulate.InfraConId;
                                byte[] smallByte = responseContent.ReadAsByteArrayAsync().Result;
                                string fStoreIdSmall = this.EbConnectionFactory.FilesDB.UploadFile(request.ImageRefId.ToString(), smallByte, request.FileCategory);

                                DbParameter[] smallImgParams =
                                {
                                                        this.EbConnectionFactory.DataDB.GetNewParameter("refid", EbDbTypes.Int32, request.ImageRefId),
                                                        this.EbConnectionFactory.DataDB.GetNewParameter("filestoreid", EbDbTypes.String, fStoreIdSmall),
                                                        this.EbConnectionFactory.DataDB.GetNewParameter("length", EbDbTypes.Int64, request.Byte.Length),
                                                        this.EbConnectionFactory.DataDB.GetNewParameter("imagequality_id", EbDbTypes.Int32, ImageQuality.small),
                                                        this.EbConnectionFactory.DataDB.GetNewParameter("filedb_con_id", EbDbTypes.Int32, this.EbConnectionFactory.FilesDB.InfraConId),
                                                        this.EbConnectionFactory.DataDB.GetNewParameter("imgmanpserid", EbDbTypes.Int32, request.ImgManpSerConId),
                                                        this.EbConnectionFactory.DataDB.GetNewParameter("is_image", EbDbTypes.Boolean, 'T'),
                                                        this.EbConnectionFactory.DataDB.GetNewParameter("userid", EbDbTypes.Int32, request.UserId)
                                };

                                var iSmallCount = this.EbConnectionFactory.DataDB.DoQuery(_dpUpdateSql, smallImgParams);

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
                return new EbMqResponse();
            }
            return new EbMqResponse { Result = true };
        }

    }

    [Restrict(InternalOnly = true)]
    public class CloudinaryInternal : EbMqBaseService
    {
        public CloudinaryInternal(IMessageProducer _mqp, IMessageQueueClient _mqc) : base(_mqp, _mqc) { }

        public EbMqResponse Post(GetImageFtpRequest request)
        {
            try
            {
                this.EbConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);

                long size = this.EbConnectionFactory.FTP.GetFileSize(request.FileUrl.Value);


                if (size > 0)
                {
                    int id = FileExists(this.EbConnectionFactory.DataDB, fname: request.FileUrl.Value.SplitOnLast('/').Last(), CustomerId: request.FileUrl.Key, IsExist: 1);
                    if (id > 0)
                        Log.Info("Counter Updated (File Exists)");
                    else
                        Log.Info("Counter Not Updated (File Exists)");


                    byte[] _byte = this.EbConnectionFactory.FTP.Download(request.FileUrl.Value);

                    if (_byte.Length > 0)
                    {
                        if (UpdateCounter(this.EbConnectionFactory.DataDB, id: id, IsFtp: 1))
                            Log.Info("Counter Updated (Byte Received)");
                        else
                            Log.Info("Counter Not Updated (Byte Received)");


                        UploadImageRequest ImageReq = new UploadImageRequest()
                        {
                            ImgQuality = ImageQuality.original,
                            FileCategory = EbFileCategory.Images,

                            Byte = _byte,

                            UserId = request.UserId,
                            SolnId = request.SolnId,

                            BToken = request.BToken,
                            RToken = request.RToken
                        };

                        ImageReq.ImageRefId = GetFileRefId(this.EbConnectionFactory.DataDB, request.UserId, request.FileUrl.Value.Split('/').Last(), request.FileUrl.Value.Split('.').Last(), String.Format(@"CustomerId: {0}", request.FileUrl.Key.ToString()), EbFileCategory.Images);

                        Console.WriteLine(@"File Recieved) ");

                        object _imgenum = null;

                        bool isImage = (Enum.TryParse(typeof(ImageTypes), request.FileUrl.Value.Split('.').Last().ToLower(), out _imgenum));

                        if (MapFilesWithUser(this.EbConnectionFactory, request.FileUrl.Key, ImageReq.ImageRefId) < 1)
                            throw new Exception("File Mapping Failed");

                        if (isImage)
                        {
                            byte[] ThumbnailBytes;

                            if (UpdateCounter(this.EbConnectionFactory.DataDB, id: id, IsImg: 1))
                                Log.Info("Counter Updated (IsImage)");

                            if (ImageReq.Byte.Length > 307200)
                            {
                                int qlty = (int)(20480000 / ImageReq.Byte.Length);

                                qlty = qlty < 7 ? 7 : qlty;

                                Log.Info("Need to Compress");
                                string Clodinaryurl = this.EbConnectionFactory.ImageManipulate.Resize
                                                    (ImageReq.Byte, ImageReq.ImageRefId.ToString(), qlty);

                                ImageReq.ImgManpSerConId = this.EbConnectionFactory.ImageManipulate.InfraConId;

                                using (var client = new HttpClient())
                                {
                                    var response = client.GetAsync(Clodinaryurl).Result;

                                    if (response.IsSuccessStatusCode)
                                    {
                                        var responseContent = response.Content;

                                        // by calling .Result you are synchronously reading the result
                                        ImageReq.Byte = responseContent.ReadAsByteArrayAsync().Result;
                                        if (UpdateCounter(this.EbConnectionFactory.DataDB, id: id, IsCloudLarge: 1))
                                            Log.Info("Counter Updated (Recieved Large Image)");
                                    }
                                    else
                                    {
                                        throw new Exception("Cloudinary Error: Image Not Downloaded");
                                    }
                                }

                            }

                            string thumbUrl = this.EbConnectionFactory.ImageManipulate.GetImgSize(ImageReq.Byte, request.FileUrl.Value.Split('/').Last(), ImageQuality.small);


                            this.MessageProducer3.Publish(ImageReq);
                            Log.Info("Pushed Image Large to Queue");


                            //TO Get thumbnail
                            using (var client = new HttpClient())
                            {
                                var response = client.GetAsync(thumbUrl).Result;

                                if (response.IsSuccessStatusCode)
                                {
                                    var responseContent = response.Content;

                                    // by calling .Result you are synchronously reading the result
                                    ThumbnailBytes = responseContent.ReadAsByteArrayAsync().Result;
                                }
                                else
                                {
                                    throw new Exception("Cloudinary Error: Transformed Image Not Available");
                                }

                            }


                            if (ThumbnailBytes.Length > 0)
                            {
                                ImageReq.Byte = ThumbnailBytes;
                                ImageReq.ImgManpSerConId = this.EbConnectionFactory.ImageManipulate.InfraConId;
                                ImageReq.ImgQuality = ImageQuality.small;

                                this.MessageProducer3.Publish(ImageReq);

                                if (UpdateCounter(this.EbConnectionFactory.DataDB, id: id, IsCloudSmall: 1))
                                    Log.Info("Counter Updated (Recieved SmallImage)");
                                Log.Info("Pushed Small to Queue after Cloudinary");
                            }


                        }
                        else
                        {
                            this.MessageProducer3.Publish(new UploadFileRequest()
                            {
                                FileRefId = ImageReq.ImageRefId,
                                FileCategory = EbFileCategory.File,

                                Byte = ImageReq.Byte,

                                UserId = request.UserId,
                                SolnId = request.SolnId,

                                BToken = request.BToken,
                                RToken = request.RToken
                            });
                            if (UpdateCounter(this.EbConnectionFactory.DataDB, id: id, IsFile: 1))
                                Log.Info("Counter Updated (File Pushed)");
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error("MQ Exception: " + e.ToString());
                return new EbMqResponse();
            }
            return new EbMqResponse { Result = true };
        }

        private int MapFilesWithUser(EbConnectionFactory connectionFactory, int CustomerId, int FileRefId)
        {
            int res = 0;
            string MapQuery = @"INSERT into customer_files (customer_id, eb_files_ref_id) values(@cust_id, @ref_id) returning id";
            DbParameter[] MapParams =
            {
                        connectionFactory.DataDB.GetNewParameter("cust_id", EbDbTypes.Int32, CustomerId),
                        connectionFactory.DataDB.GetNewParameter("ref_id", EbDbTypes.Int32, FileRefId)
            };
            var table = connectionFactory.DataDB.DoQuery(MapQuery, MapParams);
            res = (int)table.Rows[0][0];
            return res;
        }

        public bool UpdateCounter(IDatabase DataDB, int id, int IsFtp = 0, int IsCloudLarge = 0, int IsCloudSmall = 0, int IsFile = 0, int IsImg = 0)
        {
            int res = 0;

            try
            {
                string MapQuery = @"
UPDATE 
    eb_image_migration_counter
SET 
	ftp_get = eb_image_migration_counter.ftp_get + @ftp, 
	cldnry_large = eb_image_migration_counter.cldnry_large + @cldl , 
	cldnry_small = eb_image_migration_counter.cldnry_small + @clds, 
	file_upld = eb_image_migration_counter.file_upld + @file, 
	img_org = eb_image_migration_counter.img_org + @img
WHERE eb_image_migration_counter.id = @id;";
                DbParameter[] MapParams =
                {
                                DataDB.GetNewParameter("id", EbDbTypes.Int32, id),
                                DataDB.GetNewParameter("ftp", EbDbTypes.Int32, IsFtp),
                                DataDB.GetNewParameter("cldl", EbDbTypes.Int32, IsCloudLarge),
                                DataDB.GetNewParameter("clds", EbDbTypes.Int32, IsCloudSmall),
                                DataDB.GetNewParameter("file", EbDbTypes.Int32, IsFile),
                                DataDB.GetNewParameter("img", EbDbTypes.Int32, IsImg)
                    };
                res = DataDB.DoNonQuery(MapQuery, MapParams);
            }
            catch (Exception e)
            {
                Log.Error("Counter: " + e.Message);
            }
            return (res > 0);
        }

        public int FileExists(IDatabase DataDB, string fname, int CustomerId, int IsExist = 0)
        {
            int res = 0;

            try
            {
                string AddQuery = @"
UPDATE  
    eb_image_migration_counter 
SET
    is_exist = @exist
WHERE
    filename = @fname
    AND customer_id = @cid
RETURNING id";
                DbParameter[] MapParams =
                {
                                DataDB.GetNewParameter("cid", EbDbTypes.Int32, CustomerId),
                                DataDB.GetNewParameter("fname", EbDbTypes.String, fname),
                                DataDB.GetNewParameter("exist", EbDbTypes.Int32, IsExist),
                    };
                var tab = DataDB.DoQuery(AddQuery, MapParams);
                if (tab.Rows.Capacity > 0)
                    res = Convert.ToInt32(tab.Rows[0][0]);
            }
            catch (Exception e)
            {
                Log.Error("Counter: " + e.Message);
            }
            return res;
        }

        private int GetFileRefId(IDatabase datadb, int userId, string filename, string filetype, string tags, EbFileCategory ebFileCategory)
        {
            try
            {
                string IdFetchQuery =
            @"INSERT INTO
    eb_files_ref (userid, filename, filetype, tags, filecategory) 
VALUES 
    (@userid, @filename, @filetype, @tags, @filecategory) 
RETURNING id";

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
