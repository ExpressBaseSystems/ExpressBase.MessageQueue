using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.Services;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Newtonsoft.Json;
using RestSharp;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Data.Common;

namespace ExpressBase.MessageQueue.MQServices
{
    public class SlackService : EbMqBaseService
    {
        public SlackService(IMessageProducer _mqp) : base(_mqp)
        {
        }

        [Authenticate]
        public bool Post(SlackPostAsyncRequest request)
        {
            try
            {
                this.MessageProducer3.Publish(new SlackPostRequest { Payload = request.Payload, PostType = request.PostType, SolnId = request.SolnId, UserId = request.UserId });
            }
            catch (Exception e)
            {
                Log.Info("Exception: " + e.Message.ToString());
                return false;
            }
            return true;
        }

        [Authenticate]
        public bool Post(SlackAuthAsyncRequest request)
        {
            try
            {
                this.MessageProducer3.Publish(new SlackAuthRequest { IsNew = request.IsNew, SlackJson = request.SlackJson, SolnId = request.SolnId, UserId = request.UserId });
            }
            catch (Exception e)
            {
                Log.Info("Exception: " + e.Message.ToString());
                return false;
            }
            return true;
        }
    }

    [Restrict(InternalOnly = true)]
    public class SlackServiceInternal : EbMqBaseService
    {
        public SlackServiceInternal()
        {
        }

        public bool Post(SlackAuthRequest req)
        {
            EbConnectionFactory dbFactory = new EbConnectionFactory(req.SolnId, this.Redis);

            if (req.IsNew)
            {
                try
                {
                    string sql = "UPDATE eb_users SET slackjson = @slackjson WHERE id = @id";

                    var id = dbFactory.DataDB.DoQuery<Int32>(sql, new DbParameter[] {
                            dbFactory.DataDB.GetNewParameter("slackjson", EbDbTypes.Json,EbSerializers.Json_Serialize(req.SlackJson)),
                            dbFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, req.UserId)
                        });
                }
                catch (Exception e)
                {
                    Console.WriteLine("Exception: " + e.ToString());
                    return false;
                }
            }
            else
            {
                throw new NotImplementedException();
            }
            return true;
        }

        public bool Post(SlackPostRequest req)
        {
            string sql = "SELECT slackjson FROM eb_users WHERE id = @id";

            EbConnectionFactory dbFactory = new EbConnectionFactory(req.SolnId, this.Redis);

            try
            {
                var dt = dbFactory.DataDB.DoQuery(sql,
                    new DbParameter[]
                    {
                        dbFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, req.UserId)
                    });

                var json = dt.Rows[0][0];

                SlackJson slackJson = JsonConvert.DeserializeObject<SlackJson>(json.ToString());

                var client = new RestClient("https://slack.com");

                if (req.PostType == 1)
                {
                    var request = new RestRequest("api/files.upload", Method.POST);

                    request.AddParameter("token", slackJson.AccessToken);
                    request.AddParameter("user_id", slackJson.UserId);
                    request.AddParameter("team_id", slackJson.TeamId);
                    request.AddParameter("channels", req.Payload.Channel);
                    if (!string.IsNullOrEmpty(req.Payload.Text))
                        request.AddParameter("content", req.Payload.Text);

                    if (req.Payload.SlackFile != null && req.Payload.SlackFile.FileByte != null && req.Payload.SlackFile.FileByte.Length > 0)
                        request.AddFile("file", req.Payload.SlackFile.FileByte, req.Payload.SlackFile.FileName, contentType: "multipart/form-data");

                    //Execute the request
                    var res = client.ExecuteAsyncPost(request, SlackCallBack, "POST");
                }
                else if (req.PostType == 0)
                {
                    var request = new RestRequest("api/chat.postMessage", Method.POST);

                    request.AddParameter("token", slackJson.AccessToken);
                    request.AddParameter("channels", req.Payload.Channel);
                    request.AddParameter("user_id", slackJson.UserId);
                    request.AddParameter("team_id", slackJson.TeamId);
                    if (!string.IsNullOrEmpty(req.Payload.Text))
                        request.AddParameter("text", req.Payload.Text);

                    //Execute the request
                    var res = client.ExecuteAsyncPost(request, SlackCallBack, "POST");
                }
            }
            catch (Exception e)
            {
                Log.Info("Exception: " + e.Message.ToString());
                return false;
            }

            return true;
        }

        private void AuthRes(IRestResponse arg1, RestRequestAsyncHandle arg2)
        {
        }

        private void SlackCallBack(IRestResponse arg1, RestRequestAsyncHandle arg2)
        {
            //log response...
            //throw new NotImplementedException();
        }
    }
}

//To a take Screenshot of a div (Javascript)
//https://stackoverflow.com/questions/6887183/how-to-take-screenshot-of-a-div-with-javascript