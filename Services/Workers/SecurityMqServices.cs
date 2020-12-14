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
						var usr = GetUserObject(_userauth_id, true);
						try
						{
							this.ServiceStackClient.BearerToken = request.BToken;
							this.ServiceStackClient.RefreshToken = request.RToken;
							this.ServiceStackClient.RefreshTokenUri = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_GET_ACCESS_TOKEN_URL);
							this.ServiceStackClient.Post<NotifyByUserIDResponse>(new NotifyByUserIDRequest
							{
								Link = "/UserDashBoard",
								Title = $"Your roles was changed successfully",
								UsersID = usr.UserId,
								User_AuthId = usr.AuthId,
								BToken = request.BToken,
								RToken = request.RToken

							});
						}
						catch (Exception ex)
						{
							Console.WriteLine("user role changes-send notification" + ex.Message + ex.StackTrace);
						}
						try
						{
							this.ServerEventClient.BearerToken = request.BToken;
							this.ServerEventClient.RefreshToken = request.RToken;
							this.ServerEventClient.RefreshTokenUri = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_GET_ACCESS_TOKEN_URL);
							this.ServerEventClient.Post<NotifyResponse>(new NotifyUserAuthIdRequest
							{
								//ToChannel = new string[] {"file-upload" },
								Msg = "User Role Changed success",
								Selector = "cmd.userRoleChanged",
								To_UserAuthId = usr.AuthId


							});
						}
						catch (Exception exp)
						{
							Console.WriteLine("user role changes-send SSE" + exp.Message + exp.StackTrace);
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
				this.ServerEventClient.BearerToken = request.BToken;
				this.ServerEventClient.RefreshToken = request.RToken;
				this.ServerEventClient.RefreshTokenUri = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_GET_ACCESS_TOKEN_URL);
				this.ServerEventClient.Post<NotifyResponse>(new NotifyUserAuthIdRequest
				{
					//ToChannel = new string[] {"file-upload" },
					Msg = "You have been suspended from using this solution.",
					Selector = "cmd.userDisabled",
					To_UserAuthId = _userauth_id


				});
			}
			catch (Exception exp)
			{
				Console.WriteLine("user suspend/terminate - send SSE" + exp.Message + exp.StackTrace);
			}
			return MqResponse;
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
				string sql = @"INSERT INTO eb_broswer_exceptions(user_id, device_info, ip_address, error_msg, eb_created_at) VALUES ( :userid, :deviceInfo, :ipaddress, :errorMsg,  NOW());";
				DbParameter[] parameters = new DbParameter[] {
				this.EbConnectionFactory.DataDB.GetNewParameter("userid", EbDbTypes.Int32, request.UserId),
				this.EbConnectionFactory.DataDB.GetNewParameter("deviceInfo", EbDbTypes.String, request.Device_info),
				this.EbConnectionFactory.DataDB.GetNewParameter("ipaddress", EbDbTypes.String, request.Ip_address),
				this.EbConnectionFactory.DataDB.GetNewParameter("errorMsg", EbDbTypes.String, request.Error_msg)
				};
				int t = this.EbConnectionFactory.DataDB.DoNonQuery(sql, parameters.ToArray());
				if (t < 0)
				{
					Console.WriteLine("data stored into eb_broswer_Exceptions table");
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
