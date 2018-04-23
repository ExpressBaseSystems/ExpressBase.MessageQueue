using Quartz;
using ServiceStack;
using System;
using System.Threading.Tasks;

namespace ExpressBase.MessageQueue.Services.Quartz
{
    public class MyJob : IJob
    {
        public Task Execute(IJobExecutionContext context)
        {
            string[] lines = { "First line", "Second line", "Third line" };
            // WriteAllLines creates a file, writes a collection of strings to the file,
            // and then closes the file.  You do NOT need to call Flush() or Close().
            System.IO.File.WriteAllLines(@"C:\Users\USER\Desktop\WriteLines.txt", lines);

            Console.WriteLine("Inside Job");
            // ... job code goes here
            return context.AsTaskResult();
        }
    }
}