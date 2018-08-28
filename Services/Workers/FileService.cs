using ExpressBase.Common;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.EbServiceStack.ReqNRes;
using ExpressBase.Common.Enums;
using ExpressBase.Common.ServerEvents_Artifacts;
using ExpressBase.Common.ServiceClients;
using ExpressBase.Common.Structures;
using ExpressBase.MessageQueue.Services;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Drawing;
using System.Drawing.Imaging;
using System.IO;

namespace ExpressBase.MessageQueue.MQServices
{
    [Restrict(InternalOnly = true)]
    public class FileServiceInternal : BaseService
    {
        public FileServiceInternal(IMessageProducer _mqp, IMessageQueueClient _mqc, IEbServerEventClient _sec) : base(_mqp, _mqc, _sec)
        {
        }

        public string Post(UploadFileRequest request)
        {
            Log.Info("Inside Upload Img MQ Service");

            try
            {
                request.FileDetails.ObjectId = (new EbConnectionFactory(request.TenantAccountId, this.Redis)).FilesDB.UploadFile(
                    request.FileDetails.FileName,
                    request.FileDetails.MetaDataDictionary.Count != 0 ? request.FileDetails.MetaDataDictionary : new Dictionary<String, List<string>>() { },
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

                this.MessageProducer3.Publish(new FileMetaPersistRequest
                {
                    FileDetails = new FileMeta
                    {
                        ObjectId = request.FileDetails.ObjectId,
                        FileName = request.FileDetails.FileName,
                        MetaDataDictionary = (request.FileDetails.MetaDataDictionary != null) ? request.FileDetails.MetaDataDictionary : new Dictionary<String, List<string>>() { },
                        Length = request.Byte.Length,
                        FileType = request.FileDetails.FileType,
                        FileCategory = request.FileDetails.FileCategory
                    },
                    TenantAccountId = request.TenantAccountId,
                    UserId = request.UserId,
                    BToken = request.BToken,
                    RToken = request.RToken
                });


            }
            catch (Exception e)
            {
                Log.Info("Exception:" + e.ToString());
                return null;
            }
            return null;
        }

        public string Post(UploadImageRequest request)
        {
            Log.Info("Inside Upload Img MQ Service");

            try
            {
                request.ImageInfo.ObjectId = (new EbConnectionFactory(request.TenantAccountId, this.Redis)).FilesDB.UploadFile(
                    request.ImageInfo.FileName,
                    request.ImageInfo.MetaDataDictionary.Count != 0 ? request.ImageInfo.MetaDataDictionary : new Dictionary<String, List<string>>() { },
                    request.Byte,
                    request.ImageInfo.FileCategory
                    );
                this.MessageProducer3.Publish(new FileMetaPersistRequest
                {
                    FileDetails = new FileMeta
                    {
                        ObjectId = request.ImageInfo.ObjectId,
                        FileName = request.ImageInfo.FileName,
                        MetaDataDictionary = (request.ImageInfo.MetaDataDictionary != null) ? request.ImageInfo.MetaDataDictionary : new Dictionary<String, List<string>>() { },
                        Length = request.Byte.Length,
                        FileType = request.ImageInfo.FileType,
                        FileCategory = request.ImageInfo.FileCategory
                    },
                    TenantAccountId = request.TenantAccountId,
                    UserId = request.UserId,
                    BToken = request.BToken,
                    RToken = request.RToken
                });

                if (request.ImageInfo.ImageQuality == ImageQuality.original) // Works properly if Soln id doesn't contains a "_"
                {
                    this.ServerEventClient.BearerToken = request.BToken;
                    this.ServerEventClient.RefreshToken = request.RToken;
                    this.ServerEventClient.RefreshTokenUri = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_GET_ACCESS_TOKEN_URL);
                    this.ServerEventClient.Post<NotifyResponse>(new NotifyUserIdRequest
                    {
                        Msg = request.ImageInfo,
                        Selector = StaticFileConstants.UPLOADSUCCESS,
                        ToUserAuthId = request.UserAuthId,
                    });

                    this.MessageProducer3.Publish(new ImageResizeRequest
                    {
                        ImageInfo = request.ImageInfo,
                        ImageByte = request.Byte,
                        TenantAccountId = request.TenantAccountId,
                        UserId = request.UserId,
                        BToken = request.BToken,
                        RToken = request.RToken
                    });
                }
            }
            catch (Exception e)
            {
                Log.Info("Exception:" + e.ToString());
                return null;
            }
            return null;
        }

        public string Post(ImageResizeRequest request)
        {
            UploadImageRequest uploadImageRequest = new UploadImageRequest();
            uploadImageRequest.TenantAccountId = request.TenantAccountId;
            uploadImageRequest.UserId = request.UserId;

            MemoryStream ms = new MemoryStream(request.ImageByte);
            ms.Position = 0;

            try
            {
                using (Image img = Image.FromStream(ms))
                {
                    if (request.ImageInfo.FileCategory == EbFileCategory.Dp)
                    {
                        foreach (string size in Enum.GetNames(typeof(DPSizes)))
                        {
                            int sz = (int)((DPSizes)Enum.Parse(typeof(DPSizes), size));

                            Stream ImgStream = Resize(img, sz, sz);
                            request.ImageByte = new byte[ImgStream.Length];
                            ImgStream.Read(request.ImageByte, 0, request.ImageByte.Length);

                            uploadImageRequest.Byte = request.ImageByte;
                            uploadImageRequest.ImageInfo = new ImageMeta()
                            {
                                FileName = String.Format("{0}_{1}.{2}", request.ImageInfo.ObjectId.ObjectId, size, request.ImageInfo.FileType),
                                MetaDataDictionary = (request.ImageInfo.MetaDataDictionary != null) ? request.ImageInfo.MetaDataDictionary : new Dictionary<String, List<string>>() { },
                                FileType = request.ImageInfo.FileType,
                                FileCategory = EbFileCategory.Dp,
                                ImageQuality = ImageQuality.other
                            };
                            uploadImageRequest.AddAuth(request.BToken, request.RToken);
                            this.MessageProducer3.Publish(uploadImageRequest);
                        }
                    }
                    else if (request.ImageInfo.FileCategory == EbFileCategory.SolLogo)
                    {
                        foreach (string size in Enum.GetNames(typeof(LogoSizes)))
                        {
                            int sz = (int)Enum.Parse<LogoSizes>(size);

                            Stream ImgStream = Resize(img, sz, sz);
                            request.ImageByte = new byte[ImgStream.Length];
                            ImgStream.Read(request.ImageByte, 0, request.ImageByte.Length);

                            uploadImageRequest.Byte = request.ImageByte;
                            uploadImageRequest.ImageInfo = new ImageMeta()
                            {
                                FileName = String.Format("{0}_{1}.{2}", request.ImageInfo.ObjectId.ObjectId, size, request.ImageInfo.FileType),
                                MetaDataDictionary = (request.ImageInfo.MetaDataDictionary != null) ? request.ImageInfo.MetaDataDictionary : new Dictionary<String, List<string>>() { },
                                FileType = request.ImageInfo.FileType,
                                FileCategory = EbFileCategory.SolLogo,
                                ImageQuality = ImageQuality.other
                            };
                            uploadImageRequest.AddAuth(request.BToken, request.RToken);
                            this.MessageProducer3.Publish(uploadImageRequest);
                        }
                    }
                    else if (request.ImageInfo.FileCategory == EbFileCategory.LocationFile)
                    {
                        foreach (string size in Enum.GetNames(typeof(LogoSizes)))
                        {
                            int sz = (int)Enum.Parse<LogoSizes>(size);

                            Stream ImgStream = Resize(img, sz, sz);
                            request.ImageByte = new byte[ImgStream.Length];
                            ImgStream.Read(request.ImageByte, 0, request.ImageByte.Length);

                            uploadImageRequest.Byte = request.ImageByte;
                            uploadImageRequest.ImageInfo = new ImageMeta()
                            {
                                FileName = String.Format("{0}_{1}.{2}", request.ImageInfo.ObjectId.ObjectId, size, request.ImageInfo.FileType),
                                MetaDataDictionary = (request.ImageInfo.MetaDataDictionary != null) ? request.ImageInfo.MetaDataDictionary : new Dictionary<String, List<string>>() { },
                                FileType = request.ImageInfo.FileType,
                                FileCategory = EbFileCategory.LocationFile
                            };

                            uploadImageRequest.AddAuth(request.BToken, request.RToken);
                            this.MessageProducer3.Publish(uploadImageRequest);
                        }
                    }
                    else
                    {
                        foreach (string size in Enum.GetNames(typeof(ImageQuality)))
                        {

                            int sz = (int)Enum.Parse<ImageQuality>(size);

                            if (sz > 1)
                            {
                                Stream ImgStream = Resize(img, sz, sz);

                                request.ImageByte = new byte[ImgStream.Length];
                                ImgStream.Read(request.ImageByte, 0, request.ImageByte.Length);

                                uploadImageRequest.ImageInfo = new ImageMeta()
                                {
                                    FileName = String.Format("{0}_{1}.{2}", request.ImageInfo.ObjectId.ObjectId, size, request.ImageInfo.FileType),
                                    MetaDataDictionary = (request.ImageInfo.MetaDataDictionary != null) ? request.ImageInfo.MetaDataDictionary : new Dictionary<String, List<string>>() { },
                                    FileType = request.ImageInfo.FileType,
                                    FileCategory = EbFileCategory.Images,
                                    ImageQuality = Enum.Parse<ImageQuality>(size)
                                };
                                uploadImageRequest.Byte = request.ImageByte;

                                uploadImageRequest.AddAuth(request.BToken, request.RToken);
                                this.MessageProducer3.Publish(uploadImageRequest);
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Log.Info("Exception:" + e.ToString());
            }
            return null;
        }

        public string Post(FileMetaPersistRequest request)
        {
            string tag = string.Empty;
            if (request.FileDetails.MetaDataDictionary.Count != 0)
                foreach (var items in request.FileDetails.MetaDataDictionary)
                {
                    tag = string.Join(CharConstants.COMMA, items.Value);
                }

            EbConnectionFactory connectionFactory = new EbConnectionFactory(request.TenantAccountId, this.Redis);

            string sql = "INSERT INTO eb_files(filename, userid, objid, length, filetype, tags, bucketname, uploaddatetime) VALUES(@filename, @userid, @objid, @length, @filetype, @tags, @bucketname, CURRENT_TIMESTAMP) RETURNING id";
            DbParameter[] parameters =
            {
                        connectionFactory.DataDB.GetNewParameter("userid", EbDbTypes.Int32, request.UserId),
                        connectionFactory.DataDB.GetNewParameter("objid",EbDbTypes.String, request.FileDetails.ObjectId.ObjectId),
                        connectionFactory.DataDB.GetNewParameter("filename",EbDbTypes.String, request.FileDetails.FileName),
                        connectionFactory.DataDB.GetNewParameter("length",EbDbTypes.Int64, request.FileDetails.Length),
                        connectionFactory.DataDB.GetNewParameter("filetype",EbDbTypes.String, (String.IsNullOrEmpty(request.FileDetails.FileType))? StaticFileConstants.PNG : request.FileDetails.FileType),
                        connectionFactory.DataDB.GetNewParameter("tags",EbDbTypes.String, tag),
                        connectionFactory.DataDB.GetNewParameter("bucketname",EbDbTypes.String, request.FileDetails.FileCategory.ToString())
            };
            var iCount = connectionFactory.DataDB.DoQuery(sql, parameters);

            return null;
        }

        public static Stream Resize(Image img, int newWidth, int newHeight)
        {
            if (newWidth != img.Width || newHeight != img.Height)
            {
                var ratioX = (double)newWidth / img.Width;
                var ratioY = (double)newHeight / img.Height;
                var ratio = Math.Max(ratioX, ratioY);
                var width = (int)(img.Width * ratio);
                var height = (int)(img.Height * ratio);

                var newImage = new Bitmap(width, height);
                Graphics.FromImage(newImage).DrawImage(img, 0, 0, width, height);
                img = newImage;
            }

            var ms = new MemoryStream();
            img.Save(ms, ImageFormat.Png);
            ms.Position = 0;
            return ms;
        }
    }
}