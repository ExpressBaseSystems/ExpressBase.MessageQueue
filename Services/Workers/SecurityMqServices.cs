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

		public EbMqResponse Post(SaveRoleMqRequest request)
		{
			bool Permi_Change = false;
			bool User_Change = false;
			string UserList = null;
			string[] _userId = new string[] { };
			string[] Perm_arr = new string[] { };
			if (!String.IsNullOrEmpty(request.New_Permission))
			{
				Perm_arr = request.New_Permission.ToString().Split(',');
			}
			List<string> New_Permission = new List<string>(Perm_arr);
			if (New_Permission.Count != request.Old_Permission.Count)
			{
				Permi_Change = true;
			}
			else
			{
				var listOld = request.Old_Permission.Except(New_Permission).ToList().Count();
				var listNew = New_Permission.Except(request.Old_Permission).ToList().Count();
				if (listNew + listOld > 0)
				{
					Permi_Change = true;
				}
			}


			if (Permi_Change == false)
			{
				string[] User_arr = new string[] { };
				if (!String.IsNullOrEmpty(request.NewUserIds))
				{
					User_arr = request.NewUserIds.ToString().Split(',');
				}
				List<string> New_user = new List<string>(User_arr);

				var listOld = request.OldUserIds.Except(New_user).ToList();
				var listNew = New_user.Except(request.OldUserIds).ToList();
				if (listNew.Count() + listOld.Count() > 0)
				{
					User_Change = true;
					List<string> temlist = new List<string>();
					temlist.AddRange(listOld);
					temlist.AddRange(listNew);
					string[] z = temlist.ToArray();
					UserList = String.Join(",", z);
				}

			}

			if (Permi_Change || User_Change)
			{
				if (User_Change)
				{
					_userId = UserList.Split(",");
				}
				else
				{
					_userId = request.NewUserIds.Split(",");
				}

				for (int i = 0; i < _userId.Length; i++)
				{
					if (!String.IsNullOrEmpty(_userId[i]))
					{
						string _userauth_id = request.SolnId + ":" + _userId[i] + ":" + request.WhichConsole;
						User user_redis = null;
						user_redis = this.Redis.Get<User>(_userauth_id);
						if (user_redis != null)
						{
							user_redis = UpdateRedisUserObject(user_redis, request.SolnId, _userId[i], request.WhichConsole, _userauth_id);


							try
							{
								this.ServiceStackClient.BearerToken = request.BToken;
								this.ServiceStackClient.RefreshToken = request.RToken;
								this.ServiceStackClient.RefreshTokenUri = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_GET_ACCESS_TOKEN_URL);
								this.ServiceStackClient.Post<NotifyByUserIDResponse>(new NotifyByUserIDRequest
								{
									Link = "/UserDashBoard",
									Title = $"Your roles was changed successfully",
									UsersID = user_redis.UserId,
									User_AuthId = user_redis.AuthId,
									BToken = request.BToken,
									RToken = request.RToken

								});
							}
							catch (Exception ex)
							{
								Console.WriteLine("user role changes-send notification" + ex.Message + ex.StackTrace);
							}

							NotifyByUserAuthId(request.BToken, request.RToken, user_redis.AuthId, "User role changed successfully", "cmd.UpdateUserMenu");
						}
					}
				}
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
