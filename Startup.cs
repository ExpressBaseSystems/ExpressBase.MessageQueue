﻿using ExpressBase.Common;
using ExpressBase.Common.Constants;
using ExpressBase.Common.EbServiceStack.ReqNRes;
using ExpressBase.Common.ServiceClients;
using ExpressBase.Common.ServiceStack.Auth;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Funq;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ServiceStack;
using ServiceStack.Auth;
using ServiceStack.Logging;
using ServiceStack.Messaging;
using ServiceStack.RabbitMq;
using ServiceStack.Redis;
using System;
using System.IdentityModel.Tokens.Jwt;
//using Quartz;
//using ServiceStack.Quartz;
//using ExpressBase.MessageQueue.Services.Quartz;

namespace ExpressBase.MessageQueue
{
    public class Startup
    {
        public Startup(IHostingEnvironment env)
        {
            var builder = new Microsoft.Extensions.Configuration.ConfigurationBuilder()
                .SetBasePath(env.ContentRootPath)
                .AddEnvironmentVariables();
            Configuration = builder.Build();
        }

        public IConfigurationRoot Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddDataProtection(opts =>
            {
                opts.ApplicationDiscriminator = "expressbase.messagequeue";
            });
            // Add framework services.
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env, ILoggerFactory loggerFactory)
        {
            loggerFactory.AddConsole(Configuration.GetSection("Logging"));
            loggerFactory.AddDebug();

            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
                app.UseBrowserLink();
            }
            else
            {
                app.UseExceptionHandler("/Home/Error");
            }

            app.UseStaticFiles();

            app.UseServiceStack(new AppHost());
        }
    }

    public class AppHost : AppHostBase
    {
        public AppHost() : base("EXPRESSbase Message Queue", typeof(AppHost).Assembly)
        {
        }

        public override void Configure(Container container)
        {
            LogManager.LogFactory = new ConsoleLogFactory(debugEnabled: true);

            var jwtprovider = new JwtAuthProviderReader
            {
                HashAlgorithm = "RS256",
                PublicKeyXml = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_JWT_PUBLIC_KEY_XML),
#if (DEBUG)
                RequireSecureConnection = false,
                //EncryptPayload = true,
#endif
            };

            this.Plugins.Add(new AuthFeature(() => new CustomUserSession(),
                new IAuthProvider[] {
                    jwtprovider,
                }));

#if (DEBUG)
            SetConfig(new HostConfig { DebugMode = true });
#endif
            SetConfig(new HostConfig { DefaultContentType = MimeTypes.Json });

            var redisConnectionString = string.Format("redis://{0}@{1}:{2}",
               Environment.GetEnvironmentVariable(EnvironmentConstants.EB_REDIS_PASSWORD),
               Environment.GetEnvironmentVariable(EnvironmentConstants.EB_REDIS_SERVER),
               Environment.GetEnvironmentVariable(EnvironmentConstants.EB_REDIS_PORT));

            container.Register<IRedisClientsManager>(c => new RedisManagerPool(redisConnectionString));

            container.Register<IEbServerEventClient>(c => new EbServerEventClient()).ReusedWithin(ReuseScope.Request);
            container.Register<IServiceClient>(c => new JsonServiceClient(Environment.GetEnvironmentVariable(EnvironmentConstants.EB_SERVICESTACK_EXT_URL))).ReusedWithin(ReuseScope.Request);

            RabbitMqMessageFactory rabitFactory = new RabbitMqMessageFactory();
            rabitFactory.ConnectionFactory.UserName = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_RABBIT_USER);
            rabitFactory.ConnectionFactory.Password = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_RABBIT_PASSWORD);
            rabitFactory.ConnectionFactory.HostName = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_RABBIT_HOST);
            rabitFactory.ConnectionFactory.Port = Convert.ToInt32(Environment.GetEnvironmentVariable(EnvironmentConstants.EB_RABBIT_PORT));
            rabitFactory.ConnectionFactory.VirtualHost = Environment.GetEnvironmentVariable(EnvironmentConstants.EB_RABBIT_VHOST);

            var mqServer = new RabbitMqServer(rabitFactory);
            mqServer.RetryCount = 1;

            mqServer.RegisterHandler<RefreshSolutionConnectionsRequest>(base.ExecuteMessage);

            mqServer.RegisterHandler<UploadFileRequest>(base.ExecuteMessage);
            mqServer.RegisterHandler<UploadImageRequest>(base.ExecuteMessage, 3);

            mqServer.RegisterHandler<GetImageFtpRequest>(base.ExecuteMessage, 3);
            mqServer.RegisterHandler<CloudinaryUploadRequest>(base.ExecuteMessage, 3);

            mqServer.RegisterHandler<ExportApplicationRequest>(base.ExecuteMessage);
            mqServer.RegisterHandler<ImportApplicationRequest>(base.ExecuteMessage);

            mqServer.RegisterHandler<EmailServicesRequest>(base.ExecuteMessage);
            mqServer.RegisterHandler<PdfCreateServiceRequest>(base.ExecuteMessage);

            //mqServer.RegisterHandler<ImageResizeRequest>(base.ExecuteMessage);
            //mqServer.RegisterHandler<FileMetaPersistRequest>(base.ExecuteMessage);
            //mqServer.RegisterHandler<EmailServicesMqRequest>(base.ExecuteMessage);
            //mqServer.RegisterHandler<SMSSentMqRequest>(base.ExecuteMessage);
            //mqServer.RegisterHandler<SMSStatusLogMqRequest>(base.ExecuteMessage);
            //mqServer.RegisterHandler<SlackPostMqRequest>(base.ExecuteMessage);
            //mqServer.RegisterHandler<SlackAuthMqRequest>(base.ExecuteMessage);

            mqServer.Start();

            container.AddScoped<IMessageProducer, RabbitMqProducer>(serviceProvider =>
            {
                return mqServer.CreateMessageProducer() as RabbitMqProducer;
            });

            container.AddScoped<IMessageQueueClient, RabbitMqQueueClient>(serviceProvider =>
            {
                return mqServer.CreateMessageQueueClient() as RabbitMqQueueClient;
            });

            //var quartzFeature = new QuartzFeature();

            //// create a simple job trigger to repeat every minute
            //quartzFeature.RegisterJob<MyJob>(
            //    trigger =>
            //        trigger.WithSimpleSchedule(s =>
            //                s.WithInterval(TimeSpan.FromSeconds(30))
            //                    .RepeatForever()
            //            )
            //            .Build()
            //);

            //quartzFeature.RegisterJob<MyJob>(
            //    trigger =>
            //        trigger.WithDailyTimeIntervalSchedule(s => s.WithInterval(1, IntervalUnit.Minute))
            //            .Build()
            //);

            //// register the plugin
            //Plugins.Add(quartzFeature);

            this.GlobalRequestFilters.Add((req, res, requestDto) =>
            {
                ILog log = LogManager.GetLogger(GetType());

                log.Info("In GlobalRequestFilters");
                try
                {
                    log.Info("In Try");
                    if (requestDto != null)
                    {
                        log.Info("In Auth Header");
                        var auth = req.Headers[HttpHeaders.Authorization];
                        if (string.IsNullOrEmpty(auth))
                            res.ReturnAuthRequired();
                        else
                        {
                            if (req.Headers[CacheConstants.RTOKEN] != null)
                            {
                                Resolve<IEbServerEventClient>().AddAuthentication(req);
                            }
                            var jwtoken = new JwtSecurityToken(auth.Replace("Bearer", string.Empty).Trim());
                            foreach (var c in jwtoken.Claims)
                            {
                                if (c.Type == "cid" && !string.IsNullOrEmpty(c.Value))
                                {
                                    RequestContext.Instance.Items.Add(CoreConstants.SOLUTION_ID, c.Value);
                                    if (requestDto is IEbSSRequest)
                                        (requestDto as IEbSSRequest).SolnId = c.Value;
                                    if (requestDto is EbServiceStackAuthRequest)
                                        (requestDto as EbServiceStackAuthRequest).SolnId = c.Value;
                                    continue;
                                }
                                if (c.Type == "uid" && !string.IsNullOrEmpty(c.Value))
                                {
                                    RequestContext.Instance.Items.Add("UserId", Convert.ToInt32(c.Value));
                                    if (requestDto is IEbSSRequest)
                                        (requestDto as IEbSSRequest).UserId = Convert.ToInt32(c.Value);
                                    if (requestDto is EbServiceStackAuthRequest)
                                        (requestDto as EbServiceStackAuthRequest).UserId = Convert.ToInt32(c.Value);
                                    continue;
                                }
                                if (c.Type == "wc" && !string.IsNullOrEmpty(c.Value))
                                {
                                    RequestContext.Instance.Items.Add("wc", c.Value);
                                    if (requestDto is EbServiceStackAuthRequest)
                                        (requestDto as EbServiceStackAuthRequest).WhichConsole = c.Value.ToString();
                                    continue;
                                }
                                if (c.Type == "sub" && !string.IsNullOrEmpty(c.Value))
                                {
                                    RequestContext.Instance.Items.Add("sub", c.Value);
                                    if (requestDto is EbServiceStackAuthRequest)
                                        (requestDto as EbServiceStackAuthRequest).UserAuthId = c.Value.ToString();
                                    continue;
                                }
                            }
                            log.Info("Req Filter Completed");
                        }
                    }
                }
                catch (Exception e)
                {
                    log.Info("ErrorStackTraceNontokenServices..........." + e.StackTrace);
                    log.Info("ErrorMessageNontokenServices..........." + e.Message);
                    log.Info("InnerExceptionNontokenServices..........." + e.InnerException);
                }
            });
        }
    }
}

//https://github.com/ServiceStack/ServiceStack/blob/master/tests/ServiceStack.Server.Tests/Messaging/MqServerIntroTests.cs