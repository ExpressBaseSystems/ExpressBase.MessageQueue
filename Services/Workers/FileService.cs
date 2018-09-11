using ExpressBase.Common;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.EbServiceStack.ReqNRes;
using ExpressBase.Common.Enums;
using ExpressBase.Common.ServerEvents_Artifacts;
using ExpressBase.Common.ServiceClients;
using ExpressBase.Common.Structures;
using ExpressBase.MessageQueue.Services;
using Flurl.Http;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

namespace ExpressBase.MessageQueue.MQServices
{
    [Restrict(InternalOnly = true)]
    public class FileServiceInternal : EbMqBaseService
    {
        public FileServiceInternal(IMessageProducer _mqp, IMessageQueueClient _mqc, IEbServerEventClient _sec) : base(_mqp, _mqc, _sec) { }

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
    (eb_files_ref_id, filestore_sid, length, is_image)
VALUES 
    (:refid, :filestoresid, :length, :is_image) RETURNING id";

                DbParameter[] parameters =
                {
                        _ebConnectionFactory.DataDB.GetNewParameter("filestoresid",EbDbTypes.String, filestore_sid),
                        _ebConnectionFactory.DataDB.GetNewParameter("refid",EbDbTypes.Int32, request.FileRefId),
                        _ebConnectionFactory.DataDB.GetNewParameter("length",EbDbTypes.Int64, request.Byte.Length),
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
            try
            {
                EbConnectionFactory _ebConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);

                string filestore_sid = _ebConnectionFactory.FilesDB.UploadFile(request.ImageRefId.ToString(), request.Byte, request.FileCategory);

                string sql = @"
INSERT INTO
    eb_files_ref_variations 
    (eb_files_ref_id, filestore_sid, length, imagequality_id, is_image, img_manp_ser_con_id)
VALUES 
    (:refid, :filestoreid, :length, :imagequality_id, :is_image, :imgmanpserid) RETURNING id";
                DbParameter[] parameters =
                {
                        _ebConnectionFactory.DataDB.GetNewParameter("refid", EbDbTypes.Int32, request.ImageRefId),
                        _ebConnectionFactory.DataDB.GetNewParameter("filestoreid", EbDbTypes.String, filestore_sid),
                        _ebConnectionFactory.DataDB.GetNewParameter("refid", EbDbTypes.Int32, request.ImageRefId),
                        _ebConnectionFactory.DataDB.GetNewParameter("length", EbDbTypes.Int64, request.Byte.Length),
                        _ebConnectionFactory.DataDB.GetNewParameter("imgmanpserid", EbDbTypes.Int32, request.ImgManpSerConId),
                        _ebConnectionFactory.DataDB.GetNewParameter("imagequality_id", EbDbTypes.Int32, (int)request.ImgQuality),
                        _ebConnectionFactory.DataDB.GetNewParameter("is_image", EbDbTypes.Boolean, 'T')
                };

                var iCount = _ebConnectionFactory.DataDB.DoQuery(sql, parameters);

                if (iCount.Rows.Capacity > 0)
                {
                    this.ServerEventClient.BearerToken = request.BToken;
                    this.ServerEventClient.RefreshToken = request.RToken;
                    this.ServerEventClient.RefreshTokenUri = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_GET_ACCESS_TOKEN_URL);
                    this.ServerEventClient.Post<NotifyResponse>(new NotifyUserIdRequest
                    {
                        Msg = filestore_sid,
                        Selector = StaticFileConstants.UPLOADSUCCESS,
                        ToUserAuthId = request.UserAuthId,
                    });
                }
            }
            catch (Exception e)
            {
                Log.Error("UploadImage:" + e.ToString());
                return new EbMqResponse();
            }
            return new EbMqResponse { Result = true };
        }

        //public string Post(ImageResizeRequest request)
        //{
        //    UploadImageRequest uploadImageRequest = new UploadImageRequest();
        //    uploadImageRequest.TenantAccountId = request.TenantAccountId;
        //    uploadImageRequest.UserId = request.UserId;

        //    MemoryStream ms = new MemoryStream(request.ImageByte);
        //    ms.Position = 0;

        //    try
        //    {
        //        using (Image img = Image.FromStream(ms))
        //        {
        //            if (request.ImageInfo.FileCategory == EbFileCategory.Dp)
        //            {
        //                foreach (string size in Enum.GetNames(typeof(DPSizes)))
        //                {
        //                    int sz = (int)((DPSizes)Enum.Parse(typeof(DPSizes), size));

        //                    Stream ImgStream = Resize(img, sz, sz);
        //                    request.ImageByte = new byte[ImgStream.Length];
        //                    ImgStream.Read(request.ImageByte, 0, request.ImageByte.Length);

        //                    uploadImageRequest.Byte = request.ImageByte;
        //                    uploadImageRequest.ImageInfo = new ImageMeta()
        //                    {
        //                        FileName = String.Format("{0}_{1}.{2}", request.ImageInfo.FileStoreId, size, request.ImageInfo.FileType),
        //                        MetaDataDictionary = (request.ImageInfo.MetaDataDictionary != null) ? request.ImageInfo.MetaDataDictionary : new Dictionary<String, List<string>>() { },
        //                        FileType = request.ImageInfo.FileType,
        //                        FileCategory = EbFileCategory.Dp,
        //                        ImageQuality = ImageQuality.other
        //                    };
        //                    uploadImageRequest.AddAuth(request.BToken, request.RToken);
        //                    this.MessageProducer3.Publish(uploadImageRequest);
        //                }
        //            }
        //            else if (request.ImageInfo.FileCategory == EbFileCategory.SolLogo)
        //            {
        //                foreach (string size in Enum.GetNames(typeof(LogoSizes)))
        //                {
        //                    int sz = (int)Enum.Parse<LogoSizes>(size);

        //                    Stream ImgStream = Resize(img, sz, sz);
        //                    request.ImageByte = new byte[ImgStream.Length];
        //                    ImgStream.Read(request.ImageByte, 0, request.ImageByte.Length);

        //                    uploadImageRequest.Byte = request.ImageByte;
        //                    uploadImageRequest.ImageInfo = new ImageMeta()
        //                    {
        //                        FileName = String.Format("{0}_{1}.{2}", request.ImageInfo.FileStoreId, size, request.ImageInfo.FileType),
        //                        MetaDataDictionary = (request.ImageInfo.MetaDataDictionary != null) ? request.ImageInfo.MetaDataDictionary : new Dictionary<String, List<string>>() { },
        //                        FileType = request.ImageInfo.FileType,
        //                        FileCategory = EbFileCategory.SolLogo,
        //                        ImageQuality = ImageQuality.other
        //                    };
        //                    uploadImageRequest.AddAuth(request.BToken, request.RToken);
        //                    this.MessageProducer3.Publish(uploadImageRequest);
        //                }
        //            }
        //            else if (request.ImageInfo.FileCategory == EbFileCategory.LocationFile)
        //            {
        //                foreach (string size in Enum.GetNames(typeof(LogoSizes)))
        //                {
        //                    int sz = (int)Enum.Parse<LogoSizes>(size);

        //                    Stream ImgStream = Resize(img, sz, sz);
        //                    request.ImageByte = new byte[ImgStream.Length];
        //                    ImgStream.Read(request.ImageByte, 0, request.ImageByte.Length);

        //                    uploadImageRequest.Byte = request.ImageByte;
        //                    uploadImageRequest.ImageInfo = new ImageMeta()
        //                    {
        //                        FileName = String.Format("{0}_{1}.{2}", request.ImageInfo.FileStoreId, size, request.ImageInfo.FileType),
        //                        MetaDataDictionary = (request.ImageInfo.MetaDataDictionary != null) ? request.ImageInfo.MetaDataDictionary : new Dictionary<String, List<string>>() { },
        //                        FileType = request.ImageInfo.FileType,
        //                        FileCategory = EbFileCategory.LocationFile
        //                    };

        //                    uploadImageRequest.AddAuth(request.BToken, request.RToken);
        //                    this.MessageProducer3.Publish(uploadImageRequest);
        //                }
        //            }
        //            else
        //            {
        //                foreach (string size in Enum.GetNames(typeof(ImageQuality)))
        //                {

        //                    int sz = (int)Enum.Parse<ImageQuality>(size);

        //                    if (sz > 1 && sz < 500)
        //                    {
        //                        Stream ImgStream = Resize(img, sz, sz);

        //                        request.ImageByte = new byte[ImgStream.Length];
        //                        ImgStream.Read(request.ImageByte, 0, request.ImageByte.Length);

        //                        uploadImageRequest.ImageInfo = new ImageMeta()
        //                        {
        //                            FileName = String.Format("{0}_{1}.{2}", request.ImageInfo.FileStoreId, size, request.ImageInfo.FileType),
        //                            MetaDataDictionary = (request.ImageInfo.MetaDataDictionary != null) ? request.ImageInfo.MetaDataDictionary : new Dictionary<String, List<string>>() { },
        //                            FileType = request.ImageInfo.FileType,
        //                            FileCategory = EbFileCategory.Images,
        //                            ImageQuality = Enum.Parse<ImageQuality>(size),
        //                            FileRefId = request.ImageInfo.FileRefId // Not needed resized images are not updated in eb_files_ref
        //                        };
        //                        uploadImageRequest.Byte = request.ImageByte;

        //                        uploadImageRequest.AddAuth(request.BToken, request.RToken);
        //                        this.MessageProducer3.Publish(uploadImageRequest);
        //                    }
        //                }
        //            }
        //        }
        //    }
        //    catch (Exception e)
        //    {
        //        Log.Info("Exception:" + e.ToString());
        //    }
        //    return null;
        //}



        //public static Stream Resize(Image img, int newWidth, int newHeight)
        //{
        //    if (newWidth != img.Width || newHeight != img.Height)
        //    {
        //        var ratioX = (double)newWidth / img.Width;
        //        var ratioY = (double)newHeight / img.Height;
        //        var ratio = Math.Max(ratioX, ratioY);
        //        var width = (int)(img.Width * ratio);
        //        var height = (int)(img.Height * ratio);

        //        var newImage = new Bitmap(width, height);
        //        Graphics.FromImage(newImage).DrawImage(img, 0, 0, width, height);
        //        img = newImage;
        //    }

        //    var ms = new MemoryStream();
        //    img.Save(ms, ImageFormat.Png);
        //    ms.Position = 0;
        //    return ms;
        //}

    }

    [Restrict(InternalOnly = true)]
    public class CloudinaryInternal : EbMqBaseService
    {
        public CloudinaryInternal(IMessageProducer _mqp, IMessageQueueClient _mqc) : base(_mqp, _mqc) { }

        public EbMqResponse Post(GetImageFtpRequest request)
        {
            EbConnectionFactory _ebConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);

            try
            {
                byte[] _byte = _ebConnectionFactory.FTP.Download(request.FileUrl.Value);

                if (_byte.Length > 0)
                {
                    //UpdateCounter(_ebConnectionFactory.DataDB, CustomerId: request.FileUrl.Key, IsFtp: 1);

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

                    ImageReq.ImageRefId = GetFileRefId(_ebConnectionFactory.DataDB, request.UserId, request.FileUrl.Value.Split('/').Last(), request.FileUrl.Value.Split('.').Last(), String.Format(@"CustomerId: {0}", request.FileUrl.Key.ToString()), EbFileCategory.Images);

                    Console.WriteLine(@"File Recieved) ");

                    object _imgenum = null;

                    bool isImage = (Enum.TryParse(typeof(ImageTypes), request.FileUrl.Value.Split('.').Last().ToLower(), out _imgenum));

                    bool compress = ((ImageReq.Byte.Length > 614400) ? true : false);

                    if (MapFilesWithUser(_ebConnectionFactory, request.FileUrl.Key, ImageReq.ImageRefId) < 1)
                        throw new Exception("File Mapping Failed");
                    if (isImage)
                    {
                        if (compress)
                        {
                            CloudinaryUploadRequest cloudinaryUpload = new CloudinaryUploadRequest()
                            {
                                ImageRefId = ImageReq.ImageRefId.ToString(),
                                ImageBytes = ImageReq.Byte,
                                UserId = request.UserId,
                                SolnId = request.SolnId,
                                BToken = request.BToken,
                                RToken = request.RToken
                            };

                            this.MessageProducer3.Publish(cloudinaryUpload);

                            Log.Info("-------------------------------------------------Pushed to Queue to upload to Cloudinary");
                        }
                        else
                        {
                            this.MessageProducer3.Publish(ImageReq);
                            Log.Info("-------------------------------------------------Pushed Original to Queue");
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
                    }
                }
            }
            catch (Exception e)
            {
                Log.Error("MQ Exception: " + e.StackTrace);
                return new EbMqResponse();
            }
            return new EbMqResponse { Result = true };
        }

        public EbMqResponse Post(CloudinaryUploadRequest request)
        {
            try
            {
                EbConnectionFactory _ebConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);

                string url = _ebConnectionFactory.ImageManipulate.Resize
                    (request.ImageBytes, request.ImageRefId.ToString(), (int)(52428800 / request.ImageBytes.Length));

                byte[] CompressedImageBytes;
                byte[] ThumbnailBytes;

                using (var client = new HttpClient())
                {
                    var response = client.GetAsync(url).Result;

                    if (response.IsSuccessStatusCode)
                    {
                        var responseContent = response.Content;

                        // by calling .Result you are synchronously reading the result
                        CompressedImageBytes = responseContent.ReadAsByteArrayAsync().Result;
                    }
                    else
                    {
                        throw new Exception("Cloudinary Error: Image Not Downloaded");
                    }
                }

                using (var client = new HttpClient())
                {
                    var response = client.GetAsync("https://res.cloudinary.com/drifgiqrz/image/fetch/ar_1,c_fit,h_150/" + url).Result;

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
                if (CompressedImageBytes.Length > 0)
                {
                    this.MessageProducer3.Publish(new UploadImageRequest()
                    {
                        FileCategory = EbFileCategory.Images,
                        ImageRefId = request.ImageRefId.ToInt(),
                        ImgQuality = ImageQuality.original,
                        Byte = CompressedImageBytes,
                        UserId = request.UserId,
                        SolnId = request.SolnId,
                        BToken = request.BToken,
                        RToken = request.RToken
                    });
                    Log.Info("-------------------------------------------------Pushed to Queue after Cloudinary");

                }

                if (ThumbnailBytes.Length > 0)
                {
                    this.MessageProducer3.Publish(new UploadImageRequest()
                    {
                        FileCategory = EbFileCategory.Images,
                        ImageRefId = request.ImageRefId.ToInt(),
                        ImgQuality = ImageQuality.small,
                        Byte = ThumbnailBytes,
                        UserId = request.UserId,
                        SolnId = request.SolnId,
                        BToken = request.BToken,
                        RToken = request.RToken
                    });
                    Log.Info("-------------------------------------------------Pushed to Queue after Cloudinary");

                    //UpdateCounter(_ebConnectionFactory.DataDB, cust_id[0].ToInt(), IsCloudDown: 1);

                }

            }
            catch (Exception e)
            {
                Log.Error("ImageFTP:" + e.ToString());
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
        //        public bool UpdateCounter(IDatabase DataDB, int CustomerId, int IsFtp = 0, int IsCloudDown = 0, int IsCloudUp = 0, int IsUpld = 0)
        //        {
        //            int res = 0;

        //            try
        //            {
        //                string MapQuery = @"
        //INSERT INTO 
        //    eb_image_migration_counter 
        //    (customer_id, ftp_get, cldnry_up, cldnry_dwn, upld)
        //VALUES
        //    (@customer_id, @ftp, @cldw, @cldup, @upld)

        //ON CONFLICT(customer_id)
        //DO
        // UPDATE
        //   SET 
        //        ftp_get = eb_image_migration_counter.ftp_get + @ftp, 
        //        cldnry_up = eb_image_migration_counter.cldnry_up + @cldup , 
        //        cldnry_dwn = eb_image_migration_counter.cldnry_dwn + @cldw, 
        //        upld = eb_image_migration_counter.upld + @upld;";
        //                DbParameter[] MapParams =
        //                {
        //                        DataDB.GetNewParameter("customer_id", EbDbTypes.Int32, CustomerId),
        //                        DataDB.GetNewParameter("ftp", EbDbTypes.Int32, IsFtp),
        //                        DataDB.GetNewParameter("cldup", EbDbTypes.Int32, IsCloudUp),
        //                        DataDB.GetNewParameter("cldw", EbDbTypes.Int32, IsCloudDown),
        //                        DataDB.GetNewParameter("upld", EbDbTypes.Int32, IsUpld)
        //            };
        //                res = DataDB.DoNonQuery(MapQuery, MapParams);
        //            }
        //            catch (Exception e)
        //            {
        //                Log.Error("Counter: " + e.Message);
        //            }
        //            return (res > 0);
        //        }

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
