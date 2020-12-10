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
		public SecurityMqServices(IEbServerEventClient _sec, IServiceClient _ssclient, IEbConnectionFactory _dbf) : base(_sec, _ssclient, _dbf)
		{
			MqResponse = new EbMqResponse()
			{
				ReqType = this.GetType().ToString()
			};
		}
		public BrowserExceptionResponse Post(BrowserExceptionMqRequest request)
		{
			BrowserExceptionResponse Bres = new BrowserExceptionResponse();
			MessageProducer3.Publish(new BrowserExceptionRequest
			{
				Device_info = request.Device_info,
				Ip_address = request.Ip_address,
				Error_msg = request.Error_msg
			});

			Bres.Status = true;


			return Bres;
		}

		public EbMqResponse Post(SaveRoleMqRequest request)
		{
			if (!String.IsNullOrEmpty(request.ColUserIds))
			{
				string[] _userId = request.ColUserIds.Split(",");
				for (int i = 0; i < _userId.Length; i++)
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
		public EbMqResponse Post(BrowserExceptionRequest request)
		{
			try
			{
				string sql = @"INSERT INTO eb_broswer_Exceptions(user_id, device_info, ip_address, error_msg, eb_created_at) VALUES ( :userid, :deviceInfo, :ipaddress, :errorMsg,  NOW());";
				DbParameter[] parameters = new DbParameter[] {
				this.EbConnectionFactory.DataDB.GetNewParameter("userid", EbDbTypes.Int32, request.UserId),
				this.EbConnectionFactory.DataDB.GetNewParameter("deviceInfo", EbDbTypes.String, request.Device_info),
				this.EbConnectionFactory.DataDB.GetNewParameter("ipaddress", EbDbTypes.String, request.Ip_address),
				this.EbConnectionFactory.DataDB.GetNewParameter("errorMsg", EbDbTypes.String, request.Error_msg)
				};
				int t = this.EbConnectionFactory.DataDB.DoNonQuery(sql, parameters.ToArray());
				if (t > 0)
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
