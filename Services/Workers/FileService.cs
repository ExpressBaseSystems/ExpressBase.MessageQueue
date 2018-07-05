using ExpressBase.Common;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.EbServiceStack.ReqNRes;
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
                    (request.FileDetails.MetaDataDictionary.Count != 0) ?
                        request.FileDetails.MetaDataDictionary :
                        new Dictionary<String, List<string>>() { },
                    request.FileByte,
                    request.BucketName
                    ).
                    ToString();

                if (request.BucketName == StaticFileConstants.IMAGES_ORIGINAL || ((request.BucketName == StaticFileConstants.DP_IMAGES || request.BucketName == StaticFileConstants.SOL_LOGOS ) && request.FileDetails.FileName.Split(CharConstants.UNDERSCORE).Length == 2) || request.BucketName == StaticFileConstants.FILES) // Works properly if Soln id doesn't contains a "_"
                {
                    Console.WriteLine("----------------------------------------->Notified User of Upload :" + request.FileDetails.ObjectId + "\nBucket Name: " + request.BucketName);

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
                            MetaDataDictionary = (request.FileDetails.MetaDataDictionary != null) ?
                                                  request.FileDetails.MetaDataDictionary : new Dictionary<String, List<string>>() { },
                            Length = request.FileByte.Length,
                            FileType = request.FileDetails.FileType
                        },
                        BucketName = request.BucketName,
                        TenantAccountId = request.TenantAccountId,
                        UserId = request.UserId
                    });
                    if (Enum.IsDefined(typeof(ImageTypes), request.FileDetails.FileType.ToString()))
                        this.MessageProducer3.Publish(new ImageResizeRequest
                        {
                            ImageInfo = new FileMeta
                            {
                                ObjectId = request.FileDetails.ObjectId,
                                FileName = request.FileDetails.FileName,
                                MetaDataDictionary = (request.FileDetails.MetaDataDictionary != null) ?
                            request.FileDetails.MetaDataDictionary :
                            new Dictionary<String, List<string>>() { }
                            },
                            ImageByte = request.FileByte,
                            TenantAccountId = request.TenantAccountId,
                            UserId = request.UserId
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
            UploadFileRequest uploadFileRequest = new UploadFileRequest();
            uploadFileRequest.TenantAccountId = request.TenantAccountId;
            uploadFileRequest.UserId = request.UserId;

            MemoryStream ms = new MemoryStream(request.ImageByte);
            ms.Position = 0;

            try
            {
                using (Image img = Image.FromStream(ms))
                {
                    if (request.ImageInfo.FileName.StartsWith(StaticFileConstants.DP))
                    {
                        foreach (string size in Enum.GetNames(typeof(DPSizes)))
                        {
                            Stream ImgStream = Resize(img, (int)((DPSizes)Enum.Parse(typeof(DPSizes), size)), (int)((DPSizes)Enum.Parse(typeof(DPSizes), size)));
                            request.ImageByte = new byte[ImgStream.Length];
                            ImgStream.Read(request.ImageByte, 0, request.ImageByte.Length);

                            uploadFileRequest.FileByte = request.ImageByte;
                            uploadFileRequest.BucketName = StaticFileConstants.DP_IMAGES;
                            uploadFileRequest.FileDetails = new FileMeta()
                            {
                                FileName = String.Format("{0}_{1}.{2}", request.ImageInfo.FileName.Split(CharConstants.DOT)[0], ((DPSizes)Enum.Parse(typeof(DPSizes), size)).ToString(), request.ImageInfo.FileName.Split(CharConstants.DOT)[1]),
                                MetaDataDictionary = (request.ImageInfo.MetaDataDictionary != null) ?
                                    request.ImageInfo.MetaDataDictionary :
                                    new Dictionary<String, List<string>>() { },
                                FileType = request.ImageInfo.FileType
                            };
                            this.MessageProducer3.Publish(uploadFileRequest);
                        }
                    }
                    else if (request.ImageInfo.FileName.StartsWith(StaticFileConstants.LOGO))
                    {
                        foreach (string size in Enum.GetNames(typeof(LogoSizes)))
                        {
                            Stream ImgStream = Resize(img, (int)((LogoSizes)Enum.Parse(typeof(LogoSizes), size)), (int)((LogoSizes)Enum.Parse(typeof(LogoSizes), size)));
                            request.ImageByte = new byte[ImgStream.Length];
                            ImgStream.Read(request.ImageByte, 0, request.ImageByte.Length);

                            uploadFileRequest.FileByte = request.ImageByte;
                            uploadFileRequest.BucketName = StaticFileConstants.SOL_LOGOS;
                            uploadFileRequest.FileDetails = new FileMeta()
                            {
                                FileName = String.Format("{0}_{1}.{2}",
                                                request.ImageInfo.FileName.Split(CharConstants.DOT)[0],
                                                ((LogoSizes)Enum.Parse(typeof(LogoSizes), size)).ToString(),
                                                request.ImageInfo.FileName.Split(CharConstants.DOT)[1]),

                                MetaDataDictionary = (request.ImageInfo.MetaDataDictionary != null) ?
                                    request.ImageInfo.MetaDataDictionary :
                                    new Dictionary<String, List<string>>() { },
                                FileType = request.ImageInfo.FileType
                            };
                            this.MessageProducer3.Publish(uploadFileRequest);
                        }
                    }
                    else
                    {
                        foreach (string size in Enum.GetNames(typeof(ImageSizes)))
                        {
                            Stream ImgStream = Resize(img, (int)((ImageSizes)Enum.Parse(typeof(ImageSizes), size)), (int)((ImageSizes)Enum.Parse(typeof(ImageSizes), size)));

                            request.ImageByte = new byte[ImgStream.Length];
                            ImgStream.Read(request.ImageByte, 0, request.ImageByte.Length);

                            uploadFileRequest.FileDetails = new FileMeta()
                            {
                                FileName = request.ImageInfo.ObjectId + CharConstants.UNDERSCORE + size + StaticFileConstants.DOTPNG,
                                MetaDataDictionary = (request.ImageInfo.MetaDataDictionary != null) ?
                                    request.ImageInfo.MetaDataDictionary :
                                    new Dictionary<String, List<string>>() { },
                                FileType = StaticFileConstants.PNG
                            };
                            uploadFileRequest.FileByte = request.ImageByte;
                            uploadFileRequest.BucketName = string.Format("{0}_{1}", StaticFileConstants.IMAGES, size);

                            this.MessageProducer3.Publish(uploadFileRequest);
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

            string sql = "INSERT INTO eb_files(userid, objid, length, filetype, tags, bucketname, uploaddatetime) VALUES(@userid, @objid, @length, @filetype, @tags, @bucketname, CURRENT_TIMESTAMP) RETURNING id";
            DbParameter[] parameters =
            {
                        connectionFactory.DataDB.GetNewParameter("userid", EbDbTypes.Int32, request.UserId),
                        connectionFactory.DataDB.GetNewParameter("objid",EbDbTypes.String, request.FileDetails.ObjectId),
                        connectionFactory.DataDB.GetNewParameter("length",EbDbTypes.Int64, request.FileDetails.Length),
                        connectionFactory.DataDB.GetNewParameter("filetype",EbDbTypes.String, (String.IsNullOrEmpty(request.FileDetails.FileType))? StaticFileConstants.PNG : request.FileDetails.FileType),
                        connectionFactory.DataDB.GetNewParameter("tags",EbDbTypes.String, tag),
                        connectionFactory.DataDB.GetNewParameter("bucketname",EbDbTypes.String, request.BucketName)
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