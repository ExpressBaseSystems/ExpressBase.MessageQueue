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
using System.Linq;
using System.Net;

namespace ExpressBase.MessageQueue.MQServices
{
    [Restrict(InternalOnly = true)]
    public class FileServiceInternal : BaseService
    {
        public FileServiceInternal(IMessageProducer _mqp, IMessageQueueClient _mqc, IEbServerEventClient _sec) : base(_mqp, _mqc, _sec)
        {
        }

        public string Post(GetImageFtpRequest request)
        {
            EbConnectionFactory _ebConnectionFactory = new EbConnectionFactory(request.TenantAccountId, this.Redis);

            string Host = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_FTP_HOST);
            string UserName = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_FTP_USER);
            string Password = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_FTP_PASSWORD);
            FtpWebRequest req = null;
            FtpWebResponse response = null;
            try
            {
                req = (FtpWebRequest)WebRequest.Create(request.FileUrl.Value);//fullpath + name);
                req.Method = WebRequestMethods.Ftp.DownloadFile;
                req.Credentials = new NetworkCredential(UserName, Password);

                response = (FtpWebResponse)req.GetResponse();
                Console.WriteLine("File Recieved : " + request.FileUrl.Value);
                Stream responseStream = response.GetResponseStream();

                byte[] FileContents = new byte[response.ContentLength];
                if (FileContents.Length == 0)
                    throw new Exception("File returned empty");
                responseStream.ReadAsync(FileContents, 0, FileContents.Length);

                UploadImageRequest imgupreq = new UploadImageRequest();
                imgupreq.Byte = FileContents;
                imgupreq.ImageInfo = new ImageMeta();
                imgupreq.ImageInfo.FileCategory = EbFileCategory.Images;
                imgupreq.ImageInfo.FileName = request.FileUrl.Value;
                imgupreq.ImageInfo.FileType = request.FileUrl.Value.Split('.').Last();
                imgupreq.ImageInfo.ImageQuality = ImageQuality.original;
                imgupreq.ImageInfo.Length = FileContents.Length;
                imgupreq.ImageInfo.MetaDataDictionary = new Dictionary<string, List<string>>();
                imgupreq.ImageInfo.FileRefId = GetFileRefId(_ebConnectionFactory); //Passing SolnId For DB Connection (MQ)

                imgupreq.AddAuth(request.UserId, request.TenantAccountId, request.BToken, request.RToken);

                if (MapFilesWithUser(_ebConnectionFactory, request.FileUrl.Key, imgupreq.ImageInfo.FileRefId) < 1)
                    throw new Exception("File Mapping Failed");

                this.MessageProducer3.Publish(imgupreq);
                response.Close();
            }
            catch (WebException ex)
            {
                Console.WriteLine("Exception (FileName: " + request.FileUrl.Value + "): " + ex.Message);
            }
            return null;
        }

        public string Post(UploadFileRequest request)
        {
            Log.Info("Inside Upload Img MQ Service");

            try
            {
                request.FileDetails.FileStoreId = (new EbConnectionFactory(request.TenantAccountId, this.Redis)).FilesDB.UploadFile(
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

                this.MessageProducer3.Publish(new FileMetaPersistRequest
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
                MemoryStream ms = new MemoryStream(request.Byte);
                ms.Position = 0;
                byte[] TempFile;

                using (Image img = Image.FromStream(ms))
                {
                    Stream ImgStream = Resize(img, (int)ImageQuality.large, (int)ImageQuality.large);

                    TempFile = new byte[ImgStream.Length];
                    ImgStream.Read(TempFile, 0, TempFile.Length);
                }

                request.ImageInfo.FileStoreId = (new EbConnectionFactory(request.TenantAccountId, this.Redis)).FilesDB.UploadFile(
                    request.ImageInfo.FileName,
                    (request.ImageInfo.MetaDataDictionary != null) ? request.ImageInfo.MetaDataDictionary : new Dictionary<String, List<string>>() { },
                    request.Byte,
                    request.ImageInfo.FileCategory
                    );


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

                    this.MessageProducer3.Publish(new FileMetaPersistRequest
                    {
                        FileDetails = new FileMeta
                        {
                            FileStoreId = request.ImageInfo.FileStoreId,
                            FileName = request.ImageInfo.FileName,
                            MetaDataDictionary = (request.ImageInfo.MetaDataDictionary != null) ? request.ImageInfo.MetaDataDictionary : new Dictionary<String, List<string>>() { },
                            Length = request.Byte.Length,
                            FileType = request.ImageInfo.FileType,
                            FileCategory = request.ImageInfo.FileCategory,
                            FileRefId = request.ImageInfo.FileRefId
                        },
                        TenantAccountId = request.TenantAccountId,
                        UserId = request.UserId,
                        BToken = request.BToken,
                        RToken = request.RToken
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
                Log.Info("Exception:" + e.ToString() + "\n \nStackTrace: " + e.StackTrace);
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
                                FileName = String.Format("{0}_{1}.{2}", request.ImageInfo.FileStoreId, size, request.ImageInfo.FileType),
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
                                FileName = String.Format("{0}_{1}.{2}", request.ImageInfo.FileStoreId, size, request.ImageInfo.FileType),
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
                                FileName = String.Format("{0}_{1}.{2}", request.ImageInfo.FileStoreId, size, request.ImageInfo.FileType),
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

                            if (sz > 1 && sz <500)
                            {
                                Stream ImgStream = Resize(img, sz, sz);

                                request.ImageByte = new byte[ImgStream.Length];
                                ImgStream.Read(request.ImageByte, 0, request.ImageByte.Length);

                                uploadImageRequest.ImageInfo = new ImageMeta()
                                {
                                    FileName = String.Format("{0}_{1}.{2}", request.ImageInfo.FileStoreId, size, request.ImageInfo.FileType),
                                    MetaDataDictionary = (request.ImageInfo.MetaDataDictionary != null) ? request.ImageInfo.MetaDataDictionary : new Dictionary<String, List<string>>() { },
                                    FileType = request.ImageInfo.FileType,
                                    FileCategory = EbFileCategory.Images,
                                    ImageQuality = Enum.Parse<ImageQuality>(size),
                                    FileRefId = request.ImageInfo.FileRefId // Not needed resized images are not updated in eb_files_ref
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
            if (request.FileDetails.MetaDataDictionary != null)
                foreach (var items in request.FileDetails.MetaDataDictionary)
                {
                    tag = string.Join(CharConstants.COMMA, items.Value);
                }

            EbConnectionFactory connectionFactory = new EbConnectionFactory(request.TenantAccountId, this.Redis);

            string sql = "UPDATE eb_files_ref SET (filename, userid, filestore_id, length, filetype, tags, filecategory, uploadts) = (@filename, @userid, @filestoreid, @length, @filetype, @tags, @filecategory, CURRENT_TIMESTAMP) WHERE id = @refid RETURNING id";
            DbParameter[] parameters =
            {
                        connectionFactory.DataDB.GetNewParameter("userid", EbDbTypes.Int32, request.UserId),
                        connectionFactory.DataDB.GetNewParameter("filestoreid",EbDbTypes.String, request.FileDetails.FileStoreId),
                        connectionFactory.DataDB.GetNewParameter("refid",EbDbTypes.Int32, request.FileDetails.FileRefId),
                        connectionFactory.DataDB.GetNewParameter("filename",EbDbTypes.String, request.FileDetails.FileName),
                        connectionFactory.DataDB.GetNewParameter("length",EbDbTypes.Int64, request.FileDetails.Length),
                        connectionFactory.DataDB.GetNewParameter("filetype",EbDbTypes.String, (String.IsNullOrEmpty(request.FileDetails.FileType))? StaticFileConstants.PNG : request.FileDetails.FileType),
                        connectionFactory.DataDB.GetNewParameter("tags",EbDbTypes.String, tag),
                        connectionFactory.DataDB.GetNewParameter("filecategory",EbDbTypes.Int16, request.FileDetails.FileCategory)
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

        private int GetFileRefId()
        {
            string IdFetchQuery = @"INSERT into eb_files_ref(userid, filename) VALUES (1, 'test') RETURNING id";
            var table = this.EbConnectionFactory.DataDB.DoQuery(IdFetchQuery);
            int Id = (int)table.Rows[0][0];
            return Id;
        }

        private int GetFileRefId(EbConnectionFactory connectionFactory)
        {
            string IdFetchQuery = @"INSERT into eb_files_ref(userid, filename) VALUES (1, 'test') RETURNING id";
            var table = connectionFactory.DataDB.DoQuery(IdFetchQuery);
            int Id = (int)table.Rows[0][0];
            return Id;
        }

        private int MapFilesWithUser(int CustomerId, int FileRefId)
        {
            int res = 0;
            string MapQuery = @"INSERT into customer_files(customer_id, eb_files_ref_id) values(customer_id=@cust_id, eb_files_ref_id=@ref_id) returning id";
            DbParameter[] MapParams =
            {
                        this.EbConnectionFactory.DataDB.GetNewParameter("cust_id", EbDbTypes.Int32, CustomerId),
                        this.EbConnectionFactory.DataDB.GetNewParameter("ref_id", EbDbTypes.Int32, FileRefId)
            };
            var table = this.EbConnectionFactory.ObjectsDB.DoQuery(MapQuery);
            res = (int)table.Rows[0][0];
            return res;
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