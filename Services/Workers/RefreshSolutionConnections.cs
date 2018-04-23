using ExpressBase.Common;
using ExpressBase.Common.Connections;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.ServerEvents_Artifacts;
using ExpressBase.Common.ServiceClients;
using ExpressBase.MessageQueue.Services;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Data;

namespace ExpressBase.MessageQueue.MQServices
{
    public class ConnectionManagerService : BaseService
    {
        public ConnectionManagerService(IMessageProducer _mqp) : base(_mqp)
        {
        }

        [Authenticate]
        public bool Post(RefreshSolutionConnectionsBySolutionIdAsyncRequest request)
        {
            try
            {
                this.MessageProducer3.Publish(new RefreshSolutionConnectionsRequest()
                {
                    TenantAccountId = request.SolutionId,
                    UserId = request.UserId,
                    UserAuthId = request.UserAuthId,
                    BToken = (!String.IsNullOrEmpty(this.Request.Authorization)) ? this.Request.Authorization.Replace("Bearer", string.Empty).Trim() : String.Empty,
                    RToken = (!String.IsNullOrEmpty(this.Request.Headers["rToken"])) ? this.Request.Headers["rToken"] : String.Empty
                });
            }
            catch (Exception e)
            {
                return false;
            }

            return true;
        }
    }

    [Restrict(InternalOnly = true)]
    public class RefreshSolutionConnections : BaseService
    {
        public RefreshSolutionConnections(IEbServerEventClient _sec) : base(_sec)
        {
        }

        public string Post(RefreshSolutionConnectionsRequest req)
        {
            using (var con = this.InfraConnectionFactory.DataDB.GetNewConnection() as Npgsql.NpgsqlConnection)
            {
                try
                {
                    con.Open();
                    string sql = @"SELECT con_type, con_obj FROM eb_connections WHERE solution_id = @solution_id AND eb_del = 'F'";
                    DataTable dt = new DataTable();
                    var ada = new Npgsql.NpgsqlDataAdapter(sql, con);
                    ada.SelectCommand.Parameters.Add(new Npgsql.NpgsqlParameter("solution_id", NpgsqlTypes.NpgsqlDbType.Text) { Value = req.TenantAccountId });
                    ada.Fill(dt);

                    EbConnectionsConfig cons = new EbConnectionsConfig();
                    foreach (DataRow dr in dt.Rows)
                    {
                        if (dr["con_type"].ToString() == EbConnectionTypes.EbDATA.ToString())
                            cons.DataDbConnection = EbSerializers.Json_Deserialize<EbDataDbConnection>(dr["con_obj"].ToString());
                        else if (dr["con_type"].ToString() == EbConnectionTypes.EbDATA_RO.ToString())
                            cons.DataDbConnection = EbSerializers.Json_Deserialize<EbDataDbConnection>(dr["con_obj"].ToString());
                        else if (dr["con_type"].ToString() == EbConnectionTypes.EbOBJECTS.ToString())
                            cons.ObjectsDbConnection = EbSerializers.Json_Deserialize<EbObjectsDbConnection>(dr["con_obj"].ToString());
                        //else if (dr["con_type"].ToString() == EbConnectionTypes.EbFILES.ToString())
                        //    cons.FilesDbConnection = EbSerializers.Json_Deserialize<EbFilesDbConnection>(dr["con_obj"].ToString());
                        else if (dr["con_type"].ToString() == EbConnectionTypes.EbLOGS.ToString())
                            cons.LogsDbConnection = EbSerializers.Json_Deserialize<EbLogsDbConnection>(dr["con_obj"].ToString());
                        else if (dr["con_type"].ToString() == EbConnectionTypes.SMTP.ToString())
                            cons.SMTPConnection = EbSerializers.Json_Deserialize<SMTPConnection>(dr["con_obj"].ToString());
                        else if (dr["con_type"].ToString() == EbConnectionTypes.SMS.ToString())
                            cons.SMSConnection = EbSerializers.Json_Deserialize<SMSConnection>(dr["con_obj"].ToString());
                        // ... More to come
                    }

                    Redis.Set<EbConnectionsConfig>(string.Format(CoreConstants.SOLUTION_CONNECTION_REDIS_KEY, req.TenantAccountId), cons);
                }
                catch (Exception e)
                {
                    Log.Info("Exception:" + e.ToString());
                    return null;
                }
                if (!String.IsNullOrEmpty(req.UserAuthId))
                {
                    this.ServerEventClient.BearerToken = req.BToken;
                    this.ServerEventClient.RefreshToken = req.RToken;
                    this.ServerEventClient.RefreshTokenUri = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_GET_ACCESS_TOKEN_URL);
                    this.ServerEventClient.Post(new NotifyUserIdRequest()
                    {
                        Msg = "Connection Updated Succesfully",
                        Selector = "cmd.OnConnectionUpdateSuccess",
                        ToUserAuthId = req.UserAuthId
                    });
                }
                return null;
            }
        }
    }
}