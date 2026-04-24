using System.Reflection;

using Autofac;

using CMS.DataEngine;

using XperienceCommunity.DatabaseAnonymizer;
using XperienceCommunity.DatabaseAnonymizer.Services;

var builder = new ContainerBuilder();
var assemblies = Assembly.GetExecutingAssembly();

builder.RegisterType<App>();
builder.RegisterAssemblyTypes(assemblies)
    .Where(t => t.IsClass
        && !t.IsAbstract
        && typeof(IService).IsAssignableFrom(t))
    .AsImplementedInterfaces()
    .InstancePerLifetimeScope();

CMSApplication.PreInit();

await builder.Build().Resolve<App>().Run();
