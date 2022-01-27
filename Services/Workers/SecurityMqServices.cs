using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.EbServiceStack.ReqNRes;
using ExpressBase.Common.ServerEvents_Artifacts;
using ExpressBase.Common.ServiceClients;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.Services;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using ExpressBase.Security;
using ServiceStack.Auth;

namespace ExpressBase.MessageQueue.Services.Workers
{
    public class SecurityMqServices : EbMqBaseService
    {

        public SecurityMqServices(IEbServerEventClient _sec, IServiceClient _ssclient, IEbConnectionFactory _dbf, IMessageProducer _mqp) : base(_sec, _ssclient, _dbf, _mqp)
        {
            MqResponse = new EbMqResponse()
            {
                ReqType = this.GetType().ToString()
            };
        }

        [Authenticate]
        public BrowserExceptionResponse Post(BrowserExceptionMqRequest request)
        {
            BrowserExceptionResponse Bres = new BrowserExceptionResponse();
            MessageProducer3.Publish(new BrowserExceptionRequest
            {
                Device_info = request.Device_info,
                Ip_address = request.Ip_address,
                Error_msg = request.Error_msg,
                UserId = request.UserId,
                UserAuthId = request.UserAuthId,
                SolnId = request.SolnId
            });

            Bres.Status = true;


            return Bres;
        }

        private void UpdateRedisAndNotifyUser(User OldUser, string Wc, SaveRoleMqRequest request)
        {
            try
            {
                IEbConnectionFactory factory = new EbConnectionFactory(request.SolnId, Redis);
                User NewUser = User.GetUserObject(factory.DataDB, OldUser.UserId, Wc, null, null);
                OldUser.Permissions = NewUser.Permissions;
                OldUser.Roles = NewUser.Roles;
                OldUser.RoleIds = NewUser.RoleIds;
                OldUser.UserGroupIds = NewUser.UserGroupIds;
                this.Redis.Set<IUserAuth>(request.SolnId + ":" + OldUser.UserId + ":" + Wc, OldUser);

                this.ServiceStackClient.BearerToken = request.BToken;
                this.ServiceStackClient.RefreshToken = request.RToken;
                this.ServiceStackClient.RefreshTokenUri = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_GET_ACCESS_TOKEN_URL);
                this.ServiceStackClient.Post<NotifyByUserIDResponse>(new NotifyByUserIDRequest
                {
                    Link = "/UserDashBoard",
                    Title = $"Your roles was changed successfully",
                    UsersID = OldUser.UserId,
                    User_AuthId = OldUser.AuthId,
                    BToken = request.BToken,
                    RToken = request.RToken
                });

                NotifyByUserAuthId(request.BToken, request.RToken, OldUser.AuthId, "User role changed successfully", "cmd.UpdateUserMenu");
            }
            catch (Exception ex)
            {
                Console.WriteLine("user role changes-send notification" + ex.Message + ex.StackTrace);
            }
        }

        public EbMqResponse Post(SaveRoleMqRequest request)
        {
            if (request?.UserIdsToUpdate == null)
                return MqResponse;

            foreach (int uid in request.UserIdsToUpdate.Distinct())
            {
                if (uid <= 0)
                    continue;

                string uAuthId = request.SolnId + ":" + uid + ":";
                User OldUser = this.Redis.Get<User>(uAuthId + "uc");
                if (OldUser != null)
                    UpdateRedisAndNotifyUser(OldUser, "uc", request);

                OldUser = this.Redis.Get<User>(uAuthId + "mc");
                if (OldUser != null)
                    UpdateRedisAndNotifyUser(OldUser, "mc", request);
            }
            return MqResponse;
        }
        public EbMqResponse Post(SaveUserMqRequest request)
        {

            string _userauth_id = request.SolnId + ":" + request.UserId + ":uc";
            try
            {
                bool SendSSE = false;
                if ((String.IsNullOrEmpty(request.LocationAdd) || String.IsNullOrEmpty(request.LocationDelete)))
                {
                    SendSSE = true;
                }
                else
                {
                    var listOld = request.OldRole_Ids.Except(request.NewRole_Ids).ToList();
                    var listNew = request.NewRole_Ids.Except(request.OldRole_Ids).ToList();
                    if (listNew.Count() + listOld.Count() > 0)
                    {
                        SendSSE = true;
                    }
                    else
                    {
                        var OldGrpLst = request.OldUserGroups.Except(request.NewUserGroups).ToList();
                        var NewGrpLst = request.NewUserGroups.Except(request.OldUserGroups).ToList();
                        if (NewGrpLst.Count() + OldGrpLst.Count() > 0)
                        {
                            SendSSE = true;
                        }
                    }
                }

                if (SendSSE)
                {
                    if (!String.IsNullOrEmpty(request.UserId.ToString()))
                    {
                        User user_redis = null;
                        user_redis = this.Redis.Get<User>(_userauth_id);
                        if (user_redis != null)
                        {
                            user_redis = UpdateRedisUserObject(user_redis, request.SolnId, request.UserId.ToString(), request.WhichConsole, _userauth_id);
                        }
                    }
                    try
                    {
                        this.ServiceStackClient.BearerToken = request.BToken;
                        this.ServiceStackClient.RefreshToken = request.RToken;
                        this.ServiceStackClient.RefreshTokenUri = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_GET_ACCESS_TOKEN_URL);
                        this.ServiceStackClient.Post<NotifyByUserIDResponse>(new NotifyByUserIDRequest
                        {
                            Link = "/UserDashBoard",
                            Title = $"Your User preference has updated successfully",
                            UsersID = request.UserId,
                            User_AuthId = _userauth_id,
                            BToken = request.BToken,
                            RToken = request.RToken

                        });
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("user role changes-send notification" + ex.Message + ex.StackTrace);
                    }
                    NotifyByUserAuthId(request.BToken, request.RToken, _userauth_id, "User preference has updated successfully", "cmd.UpdateUserMenu");
                }


            }
            catch (Exception exp)
            {
                Console.WriteLine("user suspend/terminate - send SSE" + exp.Message + exp.StackTrace);
            }
            return MqResponse;
        }

        public EbMqResponse Post(SaveUserGroupMqRequest request)
        {
            bool SendSSE = false;
            var OldGrpLst = request.OldUserGroups.Except(request.NewUserGroups).ToList();
            var NewGrpLst = request.NewUserGroups.Except(request.OldUserGroups).ToList();
            if (NewGrpLst.Count() + OldGrpLst.Count() > 0)
            {
                SendSSE = true;
            }
            if (SendSSE)
            {
                NewGrpLst.AddRange(OldGrpLst);
                foreach (var usrId in NewGrpLst)
                {

                    string _userauth_id = request.SolnId + ":" + usrId + ":uc";
                    if (!String.IsNullOrEmpty(request.UserId.ToString()))
                    {
                        User user_redis = null;
                        user_redis = this.Redis.Get<User>(_userauth_id);
                        if (user_redis != null)
                        {
                            user_redis = UpdateRedisUserObject(user_redis, request.SolnId, usrId, request.WhichConsole, _userauth_id);
                        }
                    }
                    try
                    {
                        this.ServiceStackClient.BearerToken = request.BToken;
                        this.ServiceStackClient.RefreshToken = request.RToken;
                        this.ServiceStackClient.RefreshTokenUri = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_GET_ACCESS_TOKEN_URL);
                        this.ServiceStackClient.Post<NotifyByUserIDResponse>(new NotifyByUserIDRequest
                        {
                            Link = "/UserDashBoard",
                            Title = $"Your User Group has updated successfully",
                            UsersID = int.Parse(usrId),
                            User_AuthId = _userauth_id,
                            BToken = request.BToken,
                            RToken = request.RToken

                        });
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("user role changes-send notification" + ex.Message + ex.StackTrace);
                    }
                    NotifyByUserAuthId(request.BToken, request.RToken, _userauth_id, "User Group has updated successfully", "cmd.UpdateUserMenu");
                }
            }
            return MqResponse;
        }
        public EbMqResponse Post(SuspendUserMqRequest request)
        {
            string _userauth_id = request.SolnId + ":" + request.UserId + ":uc";
            NotifyByUserAuthId(request.BToken, request.RToken, _userauth_id, "You have been suspended from using this solution.", "cmd.userDisabled");

            return MqResponse;
        }

        public User UpdateRedisUserObject(User user_redis, string SolnId, string UserId, string WC, string UserAuth_Id)
        {
            IEbConnectionFactory factory = new EbConnectionFactory(SolnId, Redis);
            User user = User.GetUserObject(factory.DataDB, int.Parse(UserId), WC, null, null);
            user_redis.Permissions = user.Permissions;
            user_redis.Roles = user.Roles;
            user_redis.RoleIds = user.RoleIds;
            user_redis.UserGroupIds = user.UserGroupIds;
            this.Redis.Set<IUserAuth>(UserAuth_Id, user_redis);
            return user_redis;
        }

        public void NotifyByUserAuthId(string BToken, string RToken, string UserAuthId, string Msg, string Selector)
        {
            try
            {
                this.ServerEventClient.BearerToken = BToken;
                this.ServerEventClient.RefreshToken = RToken;
                this.ServerEventClient.RefreshTokenUri = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_GET_ACCESS_TOKEN_URL);
                this.ServerEventClient.Post<NotifyResponse>(new NotifyUserAuthIdRequest
                {
                    Msg = Msg,
                    Selector = Selector,
                    To_UserAuthId = UserAuthId
                });
            }
            catch (Exception exp)
            {
                Console.WriteLine("user role changes-send SSE" + exp.Message + exp.StackTrace);
            }
        }
    }

    [Restrict(InternalOnly = true)]
    public class SecurityMqInternalServices : EbMqBaseService
    {
        public SecurityMqInternalServices(IEbConnectionFactory _dbf) : base(_dbf) { }
        public EbMqResponse Post(BrowserExceptionRequest request)
        {
            try
            {
                this.EbConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);
                string sql = @"INSERT INTO eb_browser_exceptions(user_id, device_info, ip_address, error_msg, eb_created_at) VALUES ( :userid, :deviceInfo, :ipaddress, :errorMsg,  NOW());";
                DbParameter[] parameters = new DbParameter[] {
                this.EbConnectionFactory.DataDB.GetNewParameter("userid", EbDbTypes.Int32, request.UserId),
                this.EbConnectionFactory.DataDB.GetNewParameter("deviceInfo", EbDbTypes.String, request.Device_info),
                this.EbConnectionFactory.DataDB.GetNewParameter("ipaddress", EbDbTypes.String, request.Ip_address),
                this.EbConnectionFactory.DataDB.GetNewParameter("errorMsg", EbDbTypes.String, request.Error_msg)
                };
                int t = this.EbConnectionFactory.DataDB.DoNonQuery(sql, parameters.ToArray());
                if (t < 0)
                {
                    Console.WriteLine("data stored into eb_browser_exceptions table");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("BrowserExceptionRequest" + e.Message + e.StackTrace);
            }

            return MqResponse;
        }
    }
}
