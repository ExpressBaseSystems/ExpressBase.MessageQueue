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

                request.FileDetails.FileStoreId = _ebConnectionFactory.FilesDB.UploadFile(
                    request.FileDetails.FileName,
                    (request.FileDetails.MetaDataDictionary != null) ? request.FileDetails.MetaDataDictionary : new Dictionary<String, List<string>>() { },
                    request.Byte,
                    request.FileDetails.FileCategory
                    );

                this.ServerEventClient.BearerToken = request.BToken;
                this.ServerEventClient.RefreshToken = request.RToken;
                this.ServerEventClient.RefreshTokenUri = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_GET_ACCESS_TOKEN_URL);
                this.ServerEventClient.Post<NotifyResponse>(new NotifyUserIdRequest
                {
                    Msg = request.FileDetails,
                    Selector = StaticFileConstants.UPLOADSUCCESS,
                    ToUserAuthId = request.UserAuthId,
                });

                bool IsPersisted = Persist(new FileMetaPersistRequest
                {
                    FileDetails = new FileMeta
                    {
                        FileStoreId = request.FileDetails.FileStoreId,
                        FileName = request.FileDetails.FileName,
                        MetaDataDictionary = (request.FileDetails.MetaDataDictionary != null) ? request.FileDetails.MetaDataDictionary : new Dictionary<String, List<string>>() { },
                        Length = request.Byte.Length,
                        FileType = request.FileDetails.FileType,
                        FileCategory = request.FileDetails.FileCategory,
                        FileRefId = request.FileDetails.FileRefId
                    },
                    SolnId = request.SolnId,
                    UserId = request.UserId
                }, _ebConnectionFactory.DataDB);


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

                request.ImageInfo.FileStoreId = _ebConnectionFactory.FilesDB.UploadFile(
                    ((request.ImageInfo.FileName != null) ? request.ImageInfo.FileName : String.Empty),
                    (request.ImageInfo.MetaDataDictionary != null) ? request.ImageInfo.MetaDataDictionary : new Dictionary<String, List<string>>() { },
                    request.Byte,
                    request.ImageInfo.FileCategory
                    );

                this.ServerEventClient.BearerToken = request.BToken;
                this.ServerEventClient.RefreshToken = request.RToken;
                this.ServerEventClient.RefreshTokenUri = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_GET_ACCESS_TOKEN_URL);
                this.ServerEventClient.Post<NotifyResponse>(new NotifyUserIdRequest
                {
                    Msg = request.ImageInfo,
                    Selector = StaticFileConstants.UPLOADSUCCESS,
                    ToUserAuthId = request.UserAuthId,
                });

                bool IsPersisted = Persist(new FileMetaPersistRequest
                {
                    FileDetails = new FileMeta
                    {
                        FileStoreId = request.ImageInfo.FileStoreId,
                        FileName = request.ImageInfo.FileName,
                        MetaDataDictionary = (request.ImageInfo.MetaDataDictionary != null) ? request.ImageInfo.MetaDataDictionary : new Dictionary<String, List<string>>() { },
                        Length = request.Byte.Length,
                        FileType = request.ImageInfo.FileType,
                        FileCategory = request.ImageInfo.FileCategory,
                        FileRefId = request.ImageInfo.FileRefId,
                        ImgManipulationServiceId = request.ImageInfo.ImgManipulationServiceId
                    },
                    SolnId = request.SolnId,
                    UserId = request.UserId
                }, _ebConnectionFactory.DataDB);

                if (request.ImageInfo.ImageQuality == ImageQuality.large)
                    Log.Info("--------------------------------------------Image from Cloudinary Uploaded");
            }
            catch (Exception e)
            {
                Log.Error("UploadImage:" + e.ToString());
                return new EbMqResponse();
            }
            return new EbMqResponse { Result = true };
        }

        private bool Persist(FileMetaPersistRequest request, IDatabase dataDb)
        {
            string tag = string.Empty;
            if (request.FileDetails.MetaDataDictionary != null)
                foreach (var items in request.FileDetails.MetaDataDictionary)
                {
                    tag = string.Join(CharConstants.COMMA, items.Value);
                }

            string sql = "UPDATE eb_files_ref SET (filename, userid, filestore_id, length, filetype, tags, filecategory, uploadts, img_manp_ser_id) = (@filename, @userid, @filestoreid, @length, @filetype, @tags, @filecategory, CURRENT_TIMESTAMP, @imgmanpserid) WHERE id = @refid RETURNING id";
            DbParameter[] parameters =
            {
                        dataDb.GetNewParameter("userid", EbDbTypes.Int32, request.UserId),
                        dataDb.GetNewParameter("filestoreid",EbDbTypes.String, request.FileDetails.FileStoreId),
                        dataDb.GetNewParameter("refid",EbDbTypes.Int32, request.FileDetails.FileRefId),
                        dataDb.GetNewParameter("filename",EbDbTypes.String, ((request.FileDetails.FileName != null) ? request.FileDetails.FileName : String.Empty)),
                        dataDb.GetNewParameter("length",EbDbTypes.Int64, request.FileDetails.Length),
                        dataDb.GetNewParameter("filetype",EbDbTypes.String, (String.IsNullOrEmpty(request.FileDetails.FileType))? StaticFileConstants.PNG : request.FileDetails.FileType),
                        dataDb.GetNewParameter("tags",EbDbTypes.String, tag),
                        dataDb.GetNewParameter("filecategory",EbDbTypes.Int16, request.FileDetails.FileCategory),
                        dataDb.GetNewParameter("imgmanpserid", EbDbTypes.Int32, request.FileDetails.ImgManipulationServiceId)
            };
            var iCount = dataDb.DoQuery(sql, parameters);

            return (iCount.Rows.Count > 0);
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
                    UploadImageRequest ImageReq = new UploadImageRequest()
                    {
                        ImageInfo = new ImageMeta()
                        {
                            FileRefId = UploadImageRequest.GetFileRefId(_ebConnectionFactory.DataDB),
                            FileCategory = EbFileCategory.Images,
                            FileName = request.FileUrl.Value.Split('/').Last(),
                            FileType = request.FileUrl.Value.Split('.').Last(),
                            ImageQuality = ImageQuality.original,
                            Length = _byte.Length,
                            MetaDataDictionary = new Dictionary<string, List<string>>(),
                        },
                        Byte = _byte,
                        UserId = request.UserId,
                        SolnId = request.SolnId,
                        BToken = request.BToken,
                        RToken = request.RToken
                    };


                    Console.WriteLine(String.Format(@"File Recieved :{0}({1}) ", ImageReq.ImageInfo.FileName, ImageReq.ImageInfo.Length));

                    object _imgenum = null;

                    bool isImage = (Enum.TryParse(typeof(ImageTypes), ImageReq.ImageInfo.FileType.ToString().ToLower(), out _imgenum));

                    bool compress = ((ImageReq.ImageInfo.Length > 614400) ? true : false);

                    if (MapFilesWithUser(_ebConnectionFactory, request.FileUrl.Key, ImageReq.ImageInfo.FileRefId) < 1)
                        throw new Exception("File Mapping Failed");
                    if (isImage)
                    {
                        if (compress)
                        {
                            CloudinaryUploadRequest cloudinaryUpload = new CloudinaryUploadRequest()
                            {
                                ImageInfo = ImageReq.ImageInfo,
                                ImageBytes = ImageReq.Byte,
                                UserId = request.UserId,
                                SolnId = request.SolnId,
                                BToken = request.BToken,
                                RToken = request.RToken
                            };

                            cloudinaryUpload.ImageInfo.ImgManipulationServiceId = _ebConnectionFactory.ImageManipulate.InfraConId;

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
                            FileDetails = new FileMeta()
                            {
                                FileName = ImageReq.ImageInfo.FileName,
                                FileType = ImageReq.ImageInfo.FileType,
                                FileCategory = EbFileCategory.File,
                                FileRefId = ImageReq.ImageInfo.FileRefId,
                                Length = ImageReq.ImageInfo.Length
                            },
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
                    (request.ImageBytes, request.ImageInfo, (int)(52428800 / request.ImageInfo.Length));

                byte[] CompressedImageBytes;

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
                        throw new Exception();
                    }
                }

                this.MessageProducer3.Publish(new UploadImageRequest()
                {
                    ImageInfo = new ImageMeta()
                    {
                        FileName = request.ImageInfo.FileName,
                        FileCategory = request.ImageInfo.FileCategory,
                        FileType = request.ImageInfo.FileType,
                        Length = CompressedImageBytes.Length,
                        MetaDataDictionary = (request.ImageInfo.MetaDataDictionary != null) ? request.ImageInfo.MetaDataDictionary : new Dictionary<String, List<string>>() { },
                        FileRefId = request.ImageInfo.FileRefId,
                        ImageQuality = ImageQuality.original,
                        ImgManipulationServiceId = request.ImageInfo.ImgManipulationServiceId
                    },
                    Byte = CompressedImageBytes,
                    UserId = request.UserId,
                    SolnId = request.SolnId,
                    BToken = request.BToken,
                    RToken = request.RToken
                });
                Log.Info("-------------------------------------------------Pushed to Queue after Cloudinary");

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

    }
}
