using Microsoft.AspNetCore.Hosting;
using System;
using System.IO;

namespace ExpressBase.MessageQueue
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var host = new WebHostBuilder()
                .UseKestrel(options =>
                {
                    options.Limits.KeepAliveTimeout = TimeSpan.FromMinutes(7);
                })
                .UseContentRoot(Directory.GetCurrentDirectory())
                .UseUrls(urls: "http://*:41700/")
                .UseStartup<Startup>()
                .Build();

            host.Run();
        }
    }
}