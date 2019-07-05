using ExpressBase.Common;
using ExpressBase.Common.Connections;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.Messaging;
using ExpressBase.Common.ServerEvents_Artifacts;
using ExpressBase.Common.ServiceClients;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.Services;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Data;
using System.Data.Common;

namespace ExpressBase.MessageQueue.MQServices
{
    public class ConnectionManagerService : EbMqBaseService
    {
        public ConnectionManagerService(IMessageProducer _mqp) : base(_mqp)
        {
        }

        [Authenticate]
        public RefreshSolutionConnectionsAsyncResponse Post(RefreshSolutionConnectionsBySolutionIdAsyncRequest request)
        {
            RefreshSolutionConnectionsAsyncResponse res = new RefreshSolutionConnectionsAsyncResponse();

            try
            {
                this.MessageProducer3.Publish(new RefreshSolutionConnectionsRequest()
                {
                    SolnId = request.SolutionId,
                    UserId = request.UserId,
                    UserAuthId = request.UserAuthId,
                    BToken = (!String.IsNullOrEmpty(this.Request.Authorization)) ? this.Request.Authorization.Replace("Bearer", string.Empty).Trim() : String.Empty,
                    RToken = (!String.IsNullOrEmpty(this.Request.Headers["rToken"])) ? this.Request.Headers["rToken"] : String.Empty
                });
            }
            catch (Exception e)
            {
                res.ResponseStatus.Message = e.Message;
                res.ResponseStatus.StackTrace = e.StackTrace;
            }

            return res;
        }
    }

    [Restrict(InternalOnly = true)]
    public class RefreshSolutionConnections : EbMqBaseService
    {
        public RefreshSolutionConnections(IEbServerEventClient _sec) : base(_sec)
        {
        }

        public RefreshSolutionConnectionsResponse Post(RefreshSolutionConnectionsRequest request)
        {
            RefreshSolutionConnectionsResponse res = null;
            try
            {
                using (DbConnection _dbconnection = this.InfraConnectionFactory.DataDB.GetNewConnection() as Npgsql.NpgsqlConnection)
                {
                    _dbconnection.Open();
                    string sql = @"SELECT EI.id, EI.type as con_type, EC.con_obj, EC.type as config_type, EI.preference  FROM 
		                            eb_integrations EI, eb_integration_configs EC 
                                    WHERE
                                    EI.solution_id = @solution_id  AND
                                    EI.eb_integration_conf_id = EC.id AND EI.eb_del = 'F' AND EC.eb_del = 'F'; ";
                    EbConnectionsConfig cons = new EbConnectionsConfig();
                    DbParameter[] parameters = new DbParameter[] { this.InfraConnectionFactory.DataDB.GetNewParameter("solution_id", EbDbTypes.String, request.SolnId) };
                    EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(sql, parameters);
                    if (dt.Rows.Count != 0)
                    {
                        foreach (EbDataRow dr in dt.Rows)
                        {
                            if (dr["con_type"].ToString() == EbConnectionTypes.EbDATA.ToString())
                            {
                                cons.DataDbConfig = EbSerializers.Json_Deserialize<EbDbConfig>(dr["con_obj"].ToString());
                                cons.DataDbConfig.Id = (int)dr["id"];
                            }
                            //else if (dr["con_type"].ToString() == EbConnections.EbDATA_RO.ToString())
                            //{
                            //    cons.DataDbConnection = EbSerializers.Json_Deserialize<EbDataDbConnection>(dr["con_obj"].ToString());
                            //    cons.DataDbConnection.Id = (int)dr["id"];
                            //}
                            else if (dr["con_type"].ToString() == EbConnectionTypes.EbOBJECTS.ToString())
                            {
                                cons.ObjectsDbConfig = EbSerializers.Json_Deserialize<EbDbConfig>(dr["con_obj"].ToString());
                                cons.ObjectsDbConfig.Id = (int)dr["id"];
                            }
                            else if (dr["con_type"].ToString() == EbConnectionTypes.EbFILES.ToString())
                            {
                                if (cons.FilesDbConfig == null)
                                    cons.FilesDbConfig = new FilesConfigCollection();
                                EbIntegrationConf temp = EbSerializers.Json_Deserialize<EbIntegrationConf>(dr["con_obj"].ToString());
                                temp.Id = (int)dr["id"];
                                cons.FilesDbConfig.Integrations.Add(temp);
                                if ((ConPreferences)Convert.ToInt32(dr["preference"]) == ConPreferences.PRIMARY)
                                    cons.FilesDbConfig.DefaultConId = temp.Id;
                            }
                            else if (dr["con_type"].ToString() == EbConnectionTypes.EbLOGS.ToString())
                            {
                                cons.LogsDbConnection = EbSerializers.Json_Deserialize<EbDbConfig>(dr["con_obj"].ToString());
                                cons.LogsDbConnection.Id = (int)dr["id"];
                            }
                            else if (dr["con_type"].ToString() == EbConnectionTypes.SMTP.ToString())
                            {
                                if (cons.EmailConfigs == null)
                                {
                                    cons.EmailConfigs = new EmailConfigCollection();
                                }
                                EbSmtpConfig temp = EbSerializers.Json_Deserialize<EbSmtpConfig>(dr["con_obj"].ToString());
                                temp.Id = (int)dr["id"];

                                if ((ConPreferences)Convert.ToInt32(dr["preference"]) == ConPreferences.PRIMARY)
                                    cons.EmailConfigs.Primary = temp;
                                else if ((ConPreferences)Convert.ToInt32(dr["preference"]) == ConPreferences.FALLBACK)
                                    cons.EmailConfigs.FallBack = temp;
                            }
                            else if (dr["con_type"].ToString() == EbConnectionTypes.SMS.ToString())
                            {
                                if (cons.SMSConfigs == null)
                                {
                                    cons.SMSConfigs = new SmsConfigCollection();
                                }
                                EbSmsConfig temp = EbSerializers.Json_Deserialize<EbSmsConfig>(dr["con_obj"].ToString());
                                temp.Id = (int)dr["id"];

                                if ((ConPreferences)Convert.ToInt32(dr["preference"]) == ConPreferences.PRIMARY)
                                    cons.SMSConfigs.Primary = temp;
                                else if ((ConPreferences)Convert.ToInt32(dr["preference"]) == ConPreferences.FALLBACK)
                                    cons.SMSConfigs.FallBack = temp;
                            }
                            else if (dr["con_type"].ToString() == EbConnectionTypes.Cloudinary.ToString())
                            {
                                if (cons.CloudinaryConfigs == null)
                                {
                                    cons.CloudinaryConfigs = new CloudinaryConfigCollection();
                                }
                                EbCloudinaryConfig temp = EbSerializers.Json_Deserialize<EbCloudinaryConfig>(dr["con_obj"].ToString());
                                temp.Id = (int)dr["id"];
                                cons.CloudinaryConfigs.Add(temp);
                            }
                            else if (dr["con_type"].ToString() == EbConnectionTypes.MAPS.ToString())
                            {
                                if (cons.MapConfigs == null)
                                    cons.MapConfigs = new MapConfigCollection();
                                EbMapConfig temp = EbSerializers.Json_Deserialize<EbMapConfig>(dr["con_obj"].ToString());
                                temp.Id = (int)dr["id"];
                                cons.MapConfigs.Integrations.Add(temp);
                                if ((ConPreferences)Convert.ToInt32(dr["preference"]) == ConPreferences.PRIMARY)
                                    cons.MapConfigs.DefaultConId = temp.Id;
                            }
                            //else if (dr["con_type"].ToString() == EbConnectionTypes.FTP.ToString())
                            //{
                            //    cons.FTPConnection = EbSerializers.Json_Deserialize<EbFTPConnection>(dr["con_obj"].ToString());
                            //    cons.FTPConnection.Id = (int)dr["id"];
                            //}// ... More to come
                        }

                        Redis.Set<EbConnectionsConfig>(string.Format(CoreConstants.SOLUTION_INTEGRATION_REDIS_KEY, request.SolnId), cons);
                        Console.ForegroundColor = ConsoleColor.Green;
                        Console.WriteLine(request.SolnId + " ConnectionObject Updated");
                        Console.ForegroundColor = ConsoleColor.White;
                    }
                }


                //if (!String.IsNullOrEmpty(request.UserAuthId))
                //{
                //    this.ServerEventClient.BearerToken = request.BToken;
                //    this.ServerEventClient.RefreshToken = request.RToken;
                //    this.ServerEventClient.RefreshTokenUri = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_GET_ACCESS_TOKEN_URL);
                //    this.ServerEventClient.Post(new NotifyUserIdRequest()
                //    {
                //        Msg = "Connection Updated Successfully",
                //        Selector = "cmd.OnConnectionUpdateSuccess",
                //        ToUserAuthId = request.UserAuthId
                //    });
                //}
            }
            catch (Exception e)
            {
                Log.Info("Exception:" + e.ToString());
                res = new RefreshSolutionConnectionsResponse();
                res.ResponseStatus = new ResponseStatus { Message = e.Message };
            }
            return res;
        }

        //public RefreshSolutionConnectionsResponse Post(RefreshSolutionConnectionsRequest req)
        //{
        //    RefreshSolutionConnectionsResponse res = null;
        //    try
        //    {
        //        using (var con = this.InfraConnectionFactory.DataDB.GetNewConnection() as Npgsql.NpgsqlConnection)
        //        {
        //            con.Open();
        //            string sql = @"SELECT id, con_type, con_obj FROM eb_connections WHERE solution_id = @solution_id AND eb_del = 'F'";
        //            DataTable dt = new DataTable();
        //            EbConnectionsConfig cons = new EbConnectionsConfig();

        //            var ada = new Npgsql.NpgsqlDataAdapter(sql, con);
        //            ada.SelectCommand.Parameters.Add(new Npgsql.NpgsqlParameter("solution_id", NpgsqlTypes.NpgsqlDbType.Text) { Value = req.SolnId });
        //            ada.Fill(dt);

        //            if (dt.Rows.Count != 0)
        //            {
        //                EbSmsConCollection _smscollection = new EbSmsConCollection();
        //                EbMailConCollection _mailcollection = new EbMailConCollection();
        //                foreach (DataRow dr in dt.Rows)
        //                {
        //                    if (dr["con_type"].ToString() == EbConnectionTypes.EbDATA.ToString())
        //                    {
        //                        cons.DataDbConnection = EbSerializers.Json_Deserialize<EbDataDbConnection>(dr["con_obj"].ToString());
        //                        cons.DataDbConnection.Id = (int)dr["id"];
        //                    }
        //                    else if (dr["con_type"].ToString() == EbConnectionTypes.EbDATA_RO.ToString())
        //                    {
        //                        cons.DataDbConnection = EbSerializers.Json_Deserialize<EbDataDbConnection>(dr["con_obj"].ToString());
        //                        cons.DataDbConnection.Id = (int)dr["id"];
        //                    }
        //                    else if (dr["con_type"].ToString() == EbConnectionTypes.EbOBJECTS.ToString())
        //                    {
        //                        cons.ObjectsDbConnection = EbSerializers.Json_Deserialize<EbObjectsDbConnection>(dr["con_obj"].ToString());
        //                        cons.ObjectsDbConnection.Id = (int)dr["id"];
        //                    }
        //                    //else if (dr["con_type"].ToString() == EbConnectionTypes.EbFILES.ToString())
        //                    //    cons.FilesDbConnection = EbSerializers.Json_Deserialize<EbFilesDbConnection>(dr["con_obj"].ToString());
        //                    else if (dr["con_type"].ToString() == EbConnectionTypes.EbLOGS.ToString())
        //                    {
        //                        cons.LogsDbConnection = EbSerializers.Json_Deserialize<EbLogsDbConnection>(dr["con_obj"].ToString());
        //                        cons.LogsDbConnection.Id = (int)dr["id"];
        //                    }
        //                    else if (dr["con_type"].ToString() == EbConnectionTypes.SMTP.ToString())
        //                    {
        //                        EbEmail temp = EbSerializers.Json_Deserialize<EbEmail>(dr["con_obj"].ToString());
        //                        temp.Id = (int)dr["id"];
        //                        _mailcollection.Add(temp);
        //                    }
        //                    else if (dr["con_type"].ToString() == EbConnectionTypes.SMS.ToString())
        //                    {
        //                        ISMSConnection temp = EbSerializers.Json_Deserialize<ISMSConnection>(dr["con_obj"].ToString());
        //                        temp.Id = (int)dr["id"];
        //                        _smscollection.Add(temp);
        //                    }
        //                    else if (dr["con_type"].ToString() == EbConnectionTypes.Cloudinary.ToString())
        //                    {
        //                        cons.CloudinaryConnection = EbSerializers.Json_Deserialize<EbCloudinaryConnection>(dr["con_obj"].ToString());
        //                        cons.CloudinaryConnection.Id = (int)dr["id"];
        //                    }
        //                    else if (dr["con_type"].ToString() == EbConnectionTypes.FTP.ToString())
        //                    {
        //                        cons.FTPConnection = EbSerializers.Json_Deserialize<EbFTPConnection>(dr["con_obj"].ToString());
        //                        cons.FTPConnection.Id = (int)dr["id"];
        //                    }// ... More to come
        //                }
        //                cons.SMSConnections = _smscollection;
        //                cons.EmailConnections = _mailcollection;
        //                Redis.Set<EbConnectionsConfig>(string.Format(CoreConstants.SOLUTION_CONNECTION_REDIS_KEY, req.SolnId), cons);
        //            }
        //        }


        //        if (!String.IsNullOrEmpty(req.UserAuthId))
        //        {
        //            this.ServerEventClient.BearerToken = req.BToken;
        //            this.ServerEventClient.RefreshToken = req.RToken;
        //            this.ServerEventClient.RefreshTokenUri = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_GET_ACCESS_TOKEN_URL);
        //            this.ServerEventClient.Post(new NotifyUserIdRequest()
        //            {
        //                Msg = "Connection Updated Successfully",
        //                Selector = "cmd.OnConnectionUpdateSuccess",
        //                ToUserAuthId = req.UserAuthId
        //            });
        //        }
        //    }
        //    catch (Exception e)
        //    {
        //        Log.Info("Exception:" + e.ToString());
        //        res = new RefreshSolutionConnectionsResponse();
        //        res.ResponseStatus.Message = e.Message;
        //    }
        //    return res;
        //}
    }
}
