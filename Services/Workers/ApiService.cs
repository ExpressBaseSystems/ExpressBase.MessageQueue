﻿using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Net;
using ExpressBase.Common;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.Extensions;
using ExpressBase.Common.Helpers;
using ExpressBase.Common.Messaging;
using ExpressBase.Common.ServiceClients;
using ExpressBase.Common.Structures;
using ExpressBase.CoreBase.Globals;
using ExpressBase.Objects;
using ExpressBase.Objects.Helpers;
using ExpressBase.Objects.Objects;
using ExpressBase.Objects.Services;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.Security;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using RestSharp;
using ServiceStack;
using ServiceStack.Messaging;
using ServiceStack.Redis;

namespace ExpressBase.MessageQueue.Services.Workers
{
    public class ApiInternalService : EbMqBaseService
    {
        private EbApi Api { set; get; }

        public ApiInternalService(IEbConnectionFactory _dbf, IEbStaticFileClient _sfc, IServiceClient _ssclient, IMessageProducer _mqp) : base(_dbf, _sfc, _ssclient, _mqp)
        {

        }

        public Dictionary<string, object> ProcessGlobalDictionary(Dictionary<string, object> data)
        {
            Dictionary<string, object> globalParams = new Dictionary<string, object>();
            if (!(data is null))
            {
                foreach (KeyValuePair<string, object> kp in data)
                {
                    if (kp.Value == null) continue;

                    if (kp.Value is string parsed)
                    {
                        parsed = parsed.Trim();

                        if ((parsed.StartsWith("{") && parsed.EndsWith("}")) || (parsed.StartsWith("[") && parsed.EndsWith("]")))
                        {
                            string formated = parsed.Replace(@"\", string.Empty);
                            globalParams.Add(kp.Key, JObject.Parse(formated));
                        }
                        else
                            globalParams.Add(kp.Key, kp.Value);
                    }
                    else
                    {
                        globalParams.Add(kp.Key, kp.Value);
                    }
                }
            }
            return globalParams;
        }

        public ApiResponse Any(ApiMqRequest request)
        {
            try
            {
                GetApiObject(request);
                InitializeExecution();
            }
            catch (Exception e)
            {
                Console.WriteLine("---API SERVICE END POINT EX CATCH---");
                Console.WriteLine(e.Message + "\n" + e.StackTrace);
            }
            return this.Api.ApiResponse;
        }

        public void GetApiObject(ApiMqRequest request)
        {
            int UserId;
            string SolutionId;
            string UserAuthId;
            Dictionary<string, object> ApiData;

            if (request.HasRefId())
            {
                SolutionId = request.JobArgs.SolnId;
                UserAuthId = request.JobArgs.UserAuthId;
                ApiData = request.JobArgs.ApiData;
                UserId = request.JobArgs.UserId;

                try
                {
                    this.Api = new EbApi();
                    this.EbConnectionFactory = new EbConnectionFactory(SolutionId, Redis);
                    this.Api = Api.GetApi(request.JobArgs?.RefId, this.Redis, this.EbConnectionFactory.DataDB, this.EbConnectionFactory.ObjectsDB);
                    this.Api.ApiResponse = new ApiResponse();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Failed resolve api object from refid '{request.JobArgs?.RefId}'");
                    Console.WriteLine(ex.Message);
                }
            }
            else
            {
                SolutionId = request.SolnId;
                UserAuthId = request.UserAuthId;
                ApiData = request.Data;
                UserId = request.UserId;

                try
                {
                    this.EbConnectionFactory = new EbConnectionFactory(SolutionId, Redis);
                    this.Api = EbApiHelper.GetApiByName(request.Name, request.Version, this.EbConnectionFactory.ObjectsDB);
                    if (!(this.Api is null))
                    {
                        Api.Redis = this.Redis;
                        Api.ObjectsDB = this.EbConnectionFactory.ObjectsDB;
                        Api.DataDB = this.EbConnectionFactory.DataDB;
                        this.Api.ApiResponse = new ApiResponse();
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Failed resolve api object from refid '{this.Api?.RefId}'");
                    Console.WriteLine(ex.Message);
                }
            }

            if (this.Api is null)
            {
                this.Api = new EbApi { ApiResponse = new ApiResponse() };
                this.Api.ApiResponse.Message.ErrorCode = ApiErrorCode.ApiNotFound;
                this.Api.ApiResponse.Message.Status = "Api does not exist";
                this.Api.ApiResponse.Message.Description = $"Api does not exist!,";

                throw new Exception(this.Api.ApiResponse.Message.Description);
            }

            this.Api.SolutionId = SolutionId;
            this.Api.UserObject = GetUserObject(UserAuthId);

            this.Api.GlobalParams = ProcessGlobalDictionary(ApiData);
            this.Api.GlobalParams["eb_currentuser_id"] = UserId;

            if (!this.Api.GlobalParams.ContainsKey("eb_loc_id"))
            {
                this.Api.GlobalParams["eb_loc_id"] = this.Api.UserObject.Preference.DefaultLocation;
            }
        }

        private void InitializeExecution()
        {
            try
            {
                int r_count = this.Api.Resources.Count;

                while (Api.Step < r_count)
                {
                    this.Api.Resources[Api.Step].Result = this.GetResult(this.Api.Resources[Api.Step]);
                    Api.Step++;
                }

                if (this.Api.ApiResponse.Result == null)
                    this.Api.ApiResponse.Result = this.Api.Resources[Api.Step - 1].GetResult();

                this.Api.ApiResponse.Message.Status = "Success";
                this.Api.ApiResponse.Message.ErrorCode = this.Api.ApiResponse.Result == null ? ApiErrorCode.SuccessWithNoReturn : ApiErrorCode.Success;
                this.Api.ApiResponse.Message.Description = $"Api execution completed, " + this.Api.ApiResponse.Message.Description;
            }
            catch (Exception ex)
            {
                Console.WriteLine("---EXCEPTION AT API-SERVICE [InitializeExecution]---");
                Console.WriteLine(ex.Message);
            }
        }

        private object GetResult(ApiResources resource)
        {
            try
            {
                return EbApiHelper.GetResult(resource, this.Api, MessageProducer3, this, this.FileClient);
            }
            catch (Exception ex)
            {
                if (ex is ExplicitExitException)
                {
                    this.Api.ApiResponse.Message.Status = "Success";
                    this.Api.ApiResponse.Message.Description = ex.Message;
                    this.Api.ApiResponse.Message.ErrorCode = ApiErrorCode.ExplicitExit;
                }
                else
                {
                    this.Api.ApiResponse.Message.Status = "Error";
                    this.Api.ApiResponse.Message.Description = $"Failed to execute Resource '{resource.Name}' " + ex.Message;
                }

                throw new ApiException("[GetResult] ," + ex.Message);
            }
        }
    }
}
