using MongoDB.Driver;
using SplitPayment.CgibsGateway;

var builder = Host.CreateApplicationBuilder(args);
var mongoConnection = builder.Configuration["Mongo:ConnectionString"];
if (string.IsNullOrWhiteSpace(mongoConnection))
{
	throw new InvalidOperationException("Configure Mongo__ConnectionString with authentication enabled.");
}

builder.Services.AddSingleton<IMongoClient>(new MongoClient(mongoConnection));
builder.Services.AddHostedService<CgibsLedgerConsumer>();
builder.Services.AddHostedService<ReconciliationWorker>();

await builder.Build().RunAsync();
