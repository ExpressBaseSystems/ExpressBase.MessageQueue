using ExpressBase.Common;
using ExpressBase.Common.EbServiceStack.ReqNRes;
using ExpressBase.Common.ServerEvents_Artifacts;
using ExpressBase.Common.ServiceClients;
using ExpressBase.Objects.Services;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.MessageQueue.Services.Workers
{
	public class SecurityMqServices: EbMqBaseService
	{
		public SecurityMqServices(IEbServerEventClient _sec, IServiceClient _ssclient) : base( _sec, _ssclient)
		{
			MqResponse = new EbMqResponse()
			{
				ReqType = this.GetType().ToString()
			};
		}


		public EbMqResponse Post(SaveRoleMqRequest request)
		{
			if (!String.IsNullOrEmpty(request.ColUserIds))
			{
				string[] _userId = request.ColUserIds.Split(",");
				for (int i = 0; i < _userId.Length; i++)
				{
					string _userauth_id = request.SolnId + ":" + _userId[i] + ":" + request.WhichConsole;
					var usr = GetUserObject(_userauth_id,true);
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

			string _userauth_id = request.SolnId + ":" + request.UserId + ":uc" ;
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
}
