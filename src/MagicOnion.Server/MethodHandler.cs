using Grpc.Core;
using MessagePack;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices.ComTypes;
using System.Threading;
using System.Threading.Tasks;
using MagicOnion.Internal;
using Microsoft.Extensions.DependencyInjection;

namespace MagicOnion.Server
{
    public class MethodHandler : IEquatable<MethodHandler>
    {
        static int methodHandlerIdBuild = 0;
        static readonly byte[] emptyBytes = new byte[0];

        int methodHandlerId = 0;

        public string ServiceName { get; private set; }
        public string MethodName { get; private set; }
        public Type ServiceType { get; private set; }
        public MethodInfo MethodInfo { get; private set; }
        public MethodType MethodType { get; private set; }

        public ILookup<Type, Attribute> AttributeLookup { get; private set; }

        readonly IMagicOnionFilterFactory<MagicOnionFilterAttribute>[] filters;

        // options

        internal readonly bool isReturnExceptionStackTraceInErrorDetail;
        internal readonly IMagicOnionLogger logger;
        readonly bool enableCurrentContext;
        readonly IServiceProvider serviceProvider;

        // use for request handling.

        public readonly Type RequestType;
        public readonly Type UnwrappedResponseType;

        readonly bool isStreamingHub;
        readonly MessagePackSerializerOptions serializerOptions;
        readonly bool responseIsTask;

        readonly Func<ServiceContext, ValueTask> methodBody;

        // reflection cache
        static readonly MethodInfo messagePackDeserialize = typeof(MessagePackSerializer).GetMethods()
            .First(x => x.Name == "Deserialize" && x.GetParameters().Length == 3 && x.GetParameters()[0].ParameterType == typeof(ReadOnlyMemory<byte>) && x.GetParameters()[1].ParameterType == typeof(MessagePackSerializerOptions));
        static readonly MethodInfo createService = typeof(ServiceProviderHelper).GetMethod(nameof(ServiceProviderHelper.CreateService), BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static)!;

        public MethodHandler(Type classType, MethodInfo methodInfo, string methodName, MethodHandlerOptions handlerOptions, IServiceProvider serviceProvider, bool isStreamingHub)
        {
            this.methodHandlerId = Interlocked.Increment(ref methodHandlerIdBuild);
            this.serviceProvider = serviceProvider;

            var serviceInterfaceType = classType.GetInterfaces().First(x => x.GetTypeInfo().IsGenericType && x.GetGenericTypeDefinition() == typeof(IService<>)).GetGenericArguments()[0];

            this.ServiceType = classType;
            this.ServiceName = serviceInterfaceType.Name;
            this.MethodInfo = methodInfo;
            this.MethodName = methodName;
            MethodType mt;
            this.UnwrappedResponseType = UnwrapResponseType(methodInfo, out mt, out responseIsTask, out var requestType);
            this.MethodType = mt;
            this.serializerOptions = handlerOptions.SerializerOptions;
            this.isStreamingHub = isStreamingHub;

            var parameters = methodInfo.GetParameters();
            if (requestType == null)
            {
                var resolver = this.serializerOptions.Resolver;
                requestType = MagicOnionMarshallers.CreateRequestTypeAndSetResolver(classType.Name + "/" + methodInfo.Name, parameters, ref resolver);
                this.serializerOptions = this.serializerOptions.WithResolver(resolver);
            }

            this.RequestType = requestType;

            this.AttributeLookup = classType.GetCustomAttributes(true)
                .Concat(methodInfo.GetCustomAttributes(true))
                .Cast<Attribute>()
                .ToLookup(x => x.GetType());

            this.filters = handlerOptions.GlobalFilters
                .OfType<IMagicOnionFilterFactory<MagicOnionFilterAttribute>>()
                .Concat(classType.GetCustomAttributes<MagicOnionFilterAttribute>(true).Select(x => new MagicOnionServiceFilterDescriptor(x, x.Order)))
                .Concat(classType.GetCustomAttributes(true).OfType<IMagicOnionFilterFactory<MagicOnionFilterAttribute>>())
                .Concat(methodInfo.GetCustomAttributes<MagicOnionFilterAttribute>(true).Select(x => new MagicOnionServiceFilterDescriptor(x, x.Order)))
                .Concat(methodInfo.GetCustomAttributes(true).OfType<IMagicOnionFilterFactory<MagicOnionFilterAttribute>>())
                .OrderBy(x => x.Order)
                .ToArray();

            // options
            this.isReturnExceptionStackTraceInErrorDetail = handlerOptions.IsReturnExceptionStackTraceInErrorDetail;
            this.logger = serviceProvider.GetRequiredService<IMagicOnionLogger>();
            this.enableCurrentContext = handlerOptions.EnableCurrentContext;

            // prepare lambda parameters
            var createServiceMethodInfo = createService.MakeGenericMethod(classType, serviceInterfaceType);
            var contextArg = Expression.Parameter(typeof(ServiceContext), "context");
            var instance = Expression.Call(createServiceMethodInfo, contextArg);

            switch (MethodType)
            {
                case MethodType.Unary:
                    // (ServiceContext context) =>
                    // {
                    //      var request = (TRequest)context.Request;
                    //      var result = new FooService() { Context = context }.Bar(request.Item1, request.Item2);
                    //      return MethodHandlerResultHelper.SetUnaryResult(result, context);
                    // };
                    try
                    {
                        var flags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;
                        var staticFlags = BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic;

                        var requestArg = Expression.Parameter(RequestType, "request");
                        var contextRequest = Expression.Property(contextArg, typeof(ServiceContext).GetProperty("Request", flags)!);
                        var assignRequest = Expression.Assign(requestArg, Expression.Convert(contextRequest, RequestType));

                        Expression[] arguments = new Expression[parameters.Length];
                        if (parameters.Length == 1)
                        {
                            arguments[0] = requestArg;
                        }
                        else
                        {
                            for (int i = 0; i < parameters.Length; i++)
                            {
                                arguments[i] = Expression.Field(requestArg, "Item" + (i + 1));
                            }
                        }

                        var callBody = Expression.Call(instance, methodInfo, arguments);

                        var finalMethod = (responseIsTask)
                            ? typeof(MethodHandlerResultHelper).GetMethod(nameof(MethodHandlerResultHelper.SetTaskUnaryResult), staticFlags)!.MakeGenericMethod(UnwrappedResponseType)
                            : typeof(MethodHandlerResultHelper).GetMethod(nameof(MethodHandlerResultHelper.SetUnaryResult), staticFlags)!.MakeGenericMethod(UnwrappedResponseType);
                        callBody = Expression.Call(finalMethod, callBody, contextArg);

                        var body = Expression.Block(new[] { requestArg }, assignRequest, callBody);
                        var compiledBody = Expression.Lambda(body, contextArg).Compile();

                        this.methodBody = BuildMethodBodyWithFilter((Func<ServiceContext, ValueTask>)compiledBody);
                    }
                    catch (Exception ex)
                    {
                        throw new InvalidOperationException($"Can't create handler. Path:{ToString()}", ex);
                    }
                    break;
                case MethodType.ServerStreaming:
                    // (ServiceContext context) =>
                    // {
                    //      var request = MessagePackSerializer.Deserialize<T>(new ReadOnlyMemory<byte>((byte[])context.Request), context.SerializerOptions, default);
                    //      var result = new FooService() { Context = context }.Bar(request.Item1, request.Item2);
                    //      return MethodHandlerResultHelper.SerializeUnaryResult(result, context);
                    // };
                    try
                    {
                        var flags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;
                        var staticFlags = BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic;

                        var requestArg = Expression.Parameter(RequestType, "request");
                        var getSerializerOptions = Expression.Property(contextArg, typeof(ServiceContext).GetProperty("SerializerOptions", flags)!);
                        var defaultToken = Expression.Default(typeof(CancellationToken));

                        var contextRequest = Expression.Property(contextArg, typeof(ServiceContext).GetProperty("Request", flags)!);
                        var requestContent = Expression.Convert(contextRequest, typeof(byte[]));
                        var requestContentRom = Expression.New(typeof(ReadOnlyMemory<byte>).GetConstructor(new[] { typeof(byte[]) })!, requestContent);

                        var callDeserialize = Expression.Call(messagePackDeserialize.MakeGenericMethod(RequestType), requestContentRom, getSerializerOptions, defaultToken);
                        var assignRequest = Expression.Assign(requestArg, callDeserialize);

                        Expression[] arguments = new Expression[parameters.Length];
                        if (parameters.Length == 1)
                        {
                            arguments[0] = requestArg;
                        }
                        else
                        {
                            for (int i = 0; i < parameters.Length; i++)
                            {
                                arguments[i] = Expression.Field(requestArg, "Item" + (i + 1));
                            }
                        }

                        var callBody = Expression.Call(instance, methodInfo, arguments);

                        if (!responseIsTask)
                        {
                            callBody = Expression.Call(typeof(MethodHandlerResultHelper)
                                .GetMethod(nameof(MethodHandlerResultHelper.NewEmptyValueTask), staticFlags)!
                                .MakeGenericMethod(MethodInfo.ReturnType), callBody);
                        }
                        else
                        {
                            callBody = Expression.Call(typeof(MethodHandlerResultHelper)
                                .GetMethod(nameof(MethodHandlerResultHelper.TaskToEmptyValueTask), staticFlags)!
                                .MakeGenericMethod(MethodInfo.ReturnType.GetGenericArguments()[0]), callBody);
                        }

                        var body = Expression.Block(new[] { requestArg }, assignRequest, callBody);
                        var compiledBody = Expression.Lambda(body, contextArg).Compile();

                        this.methodBody = BuildMethodBodyWithFilter((Func<ServiceContext, ValueTask>)compiledBody);
                    }
                    catch (Exception ex)
                    {
                        throw new InvalidOperationException($"Can't create handler. Path:{ToString()}", ex);
                    }
                    break;
                case MethodType.ClientStreaming:
                case MethodType.DuplexStreaming:
                    if (parameters.Length != 0)
                    {
                        throw new InvalidOperationException($"{MethodType} does not support method parameters. If you need to send initial parameter, use header instead. Path:{ToString()}");
                    }

                    // (ServiceContext context) => new FooService() { Context = context }.Bar();
                    try
                    {
                        var body = Expression.Call(instance, methodInfo);
                        var staticFlags = BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic;

                        if (MethodType == MethodType.ClientStreaming)
                        {
                            var finalMethod = (responseIsTask)
                                ? typeof(MethodHandlerResultHelper).GetMethod(nameof(MethodHandlerResultHelper.SerializeTaskClientStreamingResult), staticFlags)!.MakeGenericMethod(RequestType, UnwrappedResponseType)
                                : typeof(MethodHandlerResultHelper).GetMethod(nameof(MethodHandlerResultHelper.SerializeClientStreamingResult), staticFlags)!.MakeGenericMethod(RequestType, UnwrappedResponseType);
                            body = Expression.Call(finalMethod, body, contextArg);
                        }
                        else
                        {
                            if (!responseIsTask)
                            {
                                body = Expression.Call(typeof(MethodHandlerResultHelper)
                                    .GetMethod(nameof(MethodHandlerResultHelper.NewEmptyValueTask), staticFlags)!
                                    .MakeGenericMethod(MethodInfo.ReturnType), body);
                            }
                            else
                            {
                                body = Expression.Call(typeof(MethodHandlerResultHelper)
                                    .GetMethod(nameof(MethodHandlerResultHelper.TaskToEmptyValueTask), staticFlags)!
                                    .MakeGenericMethod(MethodInfo.ReturnType.GetGenericArguments()[0]), body);
                            }
                        }

                        var compiledBody = Expression.Lambda(body, contextArg).Compile();

                        this.methodBody = BuildMethodBodyWithFilter((Func<ServiceContext, ValueTask>)compiledBody);
                    }
                    catch (Exception ex)
                    {
                        throw new InvalidOperationException($"Can't create handler. Path:{ToString()}", ex);
                    }
                    break;
                default:
                    throw new InvalidOperationException("Unknown MethodType:" + MethodType + $"Path:{ToString()}");
            }
        }

        // non-filtered.
        public byte[] BoxedSerialize(object requestValue)
        {
            return MessagePackSerializer.Serialize(RequestType, requestValue, serializerOptions);
        }

        public object BoxedDeserialize(byte[] responseValue)
        {
            return MessagePackSerializer.Deserialize(UnwrappedResponseType, responseValue, serializerOptions);
        }

        static Type UnwrapResponseType(MethodInfo methodInfo, out MethodType methodType, out bool responseIsTask, out Type? requestTypeIfExists)
        {
            var t = methodInfo.ReturnType;
            if (!t.GetTypeInfo().IsGenericType) throw new Exception($"Invalid return type, path:{methodInfo.DeclaringType!.Name + "/" + methodInfo.Name} type:{methodInfo.ReturnType.Name}");

            // Task<Unary<T>>
            if (t.GetGenericTypeDefinition() == typeof(Task<>))
            {
                responseIsTask = true;
                t = t.GetGenericArguments()[0];
            }
            else
            {
                responseIsTask = false;
            }

            // Unary<T>
            var returnType = t.GetGenericTypeDefinition();
            if (returnType == typeof(UnaryResult<>))
            {
                methodType = MethodType.Unary;
                requestTypeIfExists = default;
                return t.GetGenericArguments()[0];
            }
            else if (returnType == typeof(ClientStreamingResult<,>))
            {
                methodType = MethodType.ClientStreaming;
                var genArgs = t.GetGenericArguments();
                requestTypeIfExists = genArgs[0];
                return genArgs[1];
            }
            else if (returnType == typeof(ServerStreamingResult<>))
            {
                methodType = MethodType.ServerStreaming;
                requestTypeIfExists = default;
                return t.GetGenericArguments()[0];
            }
            else if (returnType == typeof(DuplexStreamingResult<,>))
            {
                methodType = MethodType.DuplexStreaming;
                var genArgs = t.GetGenericArguments();
                requestTypeIfExists = genArgs[0];
                return genArgs[1];
            }
            else
            {
                throw new Exception($"Invalid return type, path:{methodInfo.DeclaringType!.Name + "/" + methodInfo.Name} type:{methodInfo.ReturnType.Name}");
            }
        }

        Func<ServiceContext, ValueTask> BuildMethodBodyWithFilter(Func<ServiceContext, ValueTask> methodBody)
        {
            Func<ServiceContext, ValueTask> next = methodBody;

            foreach (var filterFactory in this.filters.Reverse())
            {
                var newFilter = filterFactory.CreateInstance(serviceProvider);
                next = new InvokeHelper<ServiceContext, Func<ServiceContext, ValueTask>>(newFilter.Invoke, next).GetDelegate();
            }

            return next;
        }

        void BindUnaryHandler(ServiceBinderBase binder)
        {
            // NOTE: ServiceBinderBase.AddMethod has `class` generic constraint.
            //       We need to box an instance of the value type.
            var rawRequestType = RequestType.IsValueType ? typeof(Box<>).MakeGenericType(RequestType) : RequestType;
            var rawResponseType = UnwrappedResponseType.IsValueType ? typeof(Box<>).MakeGenericType(UnwrappedResponseType) : UnwrappedResponseType;

            MethodInfo bindMethod;
            if (MethodInfo.GetParameters().Any())
            {
                bindMethod = this.GetType().GetMethod(nameof(BindUnaryHandler_Raw), BindingFlags.Instance | BindingFlags.NonPublic)!;
            }
            else
            {
                // WORKAROUND: Prior to MagicOnion 5.0, the request type for the parameter-less method was byte[].
                //             DynamicClient sends byte[], but GeneratedClient sends Nil, which is incompatible,
                //             so as a special case we do not serialize/deserialize and always convert to a fixed values.
                bindMethod = this.GetType().GetMethod(nameof(BindUnaryHandler_Ignore), BindingFlags.Instance | BindingFlags.NonPublic)!;
            }
            ((Action<ServiceBinderBase>)bindMethod.MakeGenericMethod(RequestType, UnwrappedResponseType, rawRequestType, rawResponseType).CreateDelegate(typeof(Action<ServiceBinderBase>), this))(binder);
        }

        void BindUnaryHandler_Ignore<TRequest /* Ignore */, TResponse, TRawRequest /* Ignore */, TRawResponse>(ServiceBinderBase binder)
            where TRawResponse : class
        {
            var method = GrpcMethodHelper.CreateMethod<TResponse, TRawResponse>(this.MethodType, this.ServiceName, this.MethodName, serializerOptions);
#pragma warning disable CS8604
            binder.AddMethod(new MagicOnionServerMethod<Box<Nil>, TRawResponse>(method.Method, this), 
                async (request, context) => method.ToRawResponse(await UnaryServerMethod<Nil, TResponse>(method.FromRawRequest(request), context)));
#pragma warning restore CS8604
        }

        void BindUnaryHandler_Raw<TRequest, TResponse, TRawRequest, TRawResponse>(ServiceBinderBase binder)
            where TRawRequest : class
            where TRawResponse : class
        {
            var method = GrpcMethodHelper.CreateMethod<TRequest, TResponse, TRawRequest, TRawResponse>(this.MethodType, this.ServiceName, this.MethodName, serializerOptions);
#pragma warning disable CS8604
            binder.AddMethod(new MagicOnionServerMethod<TRawRequest, TRawResponse>(method.Method, this), 
                async (request, context) => method.ToRawResponse(await UnaryServerMethod<TRequest, TResponse>(method.FromRawRequest(request), context)));
#pragma warning restore CS8604
        }

        void BindDuplexStreamingHandler(ServiceBinderBase binder, bool isStreamingHub)
        {
            // NOTE: ServiceBinderBase.AddMethod has `class` generic constraint.
            //       We need to box an instance of the value type.
            var rawRequestType = RequestType.IsValueType ? typeof(Box<>).MakeGenericType(RequestType) : RequestType;
            var rawResponseType = UnwrappedResponseType.IsValueType ? typeof(Box<>).MakeGenericType(UnwrappedResponseType) : UnwrappedResponseType;

            var bindMethod = isStreamingHub
                ? this.GetType().GetMethod(nameof(BindDuplexStreamingHandler_StreamingHub), BindingFlags.Instance | BindingFlags.NonPublic)!
                : this.GetType().GetMethod(nameof(BindDuplexStreamingHandler_Standard), BindingFlags.Instance | BindingFlags.NonPublic)!;
            ((Action<ServiceBinderBase>)bindMethod.MakeGenericMethod(RequestType, UnwrappedResponseType, rawRequestType, rawResponseType).CreateDelegate(typeof(Action<ServiceBinderBase>), this))(binder);
        }

        void BindDuplexStreamingHandler_Standard<TRequest, TResponse, TRawRequest, TRawResponse>(ServiceBinderBase binder)
            where TRawRequest : class
            where TRawResponse : class
        {
            var method = GrpcMethodHelper.CreateMethod<TRequest, TResponse, TRawRequest, TRawResponse>(this.MethodType, this.ServiceName, this.MethodName, serializerOptions);
#pragma warning disable CS8604
            binder.AddMethod(new MagicOnionServerMethod<TRawRequest, TRawResponse>(method.Method, this),
                async (request, response, context) => await DuplexStreamingServerMethod<TRequest, TResponse>(
                    UnboxAsyncStreamReader.Create<TRequest, TRawRequest>(request),
                    BoxServerStreamWriter.Create<TResponse, TRawResponse>(response),
                    context
                ));
#pragma warning restore CS8604
        }
        
        void BindDuplexStreamingHandler_StreamingHub<TRequest, TResponse, TRawRequest, TRawResponse>(ServiceBinderBase binder)
            where TRawRequest : class
            where TRawResponse : class
        {
            // StreamingHub uses the special marshallers for streaming messages serialization.
            var method = new GrpcMethodHelper.MagicOnionMethod<TRequest, TResponse, TRawRequest, TRawResponse>(new Method<TRawRequest, TRawResponse>(
                this.MethodType,
                this.ServiceName,
                this.MethodName,
                (Marshaller<TRawRequest>)(object)Hubs.StreamingHubMarshaller.CreateForRequest(this, serializerOptions),
                (Marshaller<TRawResponse>)(object)Hubs.StreamingHubMarshaller.CreateForResponse(this, serializerOptions)
            ));
#pragma warning disable CS8604
            binder.AddMethod(new MagicOnionServerMethod<TRawRequest, TRawResponse>(method.Method, this),
                async (request, response, context) => await DuplexStreamingServerMethod<TRequest, TResponse>(
                    UnboxAsyncStreamReader.Create<TRequest, TRawRequest>(request),
                    BoxServerStreamWriter.Create<TResponse, TRawResponse>(response),
                    context
                ));
#pragma warning restore CS8604
        }

        internal void BindHandler(ServiceBinderBase binder)
        {
            switch (this.MethodType)
            {
                case MethodType.Unary:
                    {
                        BindUnaryHandler(binder);
                    }
                    break;
                case MethodType.ClientStreaming:
                    {
                        var method = new Method<byte[], byte[]>(this.MethodType, this.ServiceName, this.MethodName, MagicOnionMarshallers.ThroughMarshaller, MagicOnionMarshallers.ThroughMarshaller);
                        var genericMethod = this.GetType()
                            .GetMethod(nameof(ClientStreamingServerMethod), BindingFlags.Instance | BindingFlags.NonPublic)!
                            .MakeGenericMethod(RequestType, UnwrappedResponseType);
                        var handler = (ClientStreamingServerMethod<byte[], byte[]>)genericMethod.CreateDelegate(typeof(ClientStreamingServerMethod<byte[], byte[]>), this);
                        binder.AddMethod(new MagicOnionServerMethod<byte[], byte[]>(method, this), handler);
                    }
                    break;
                case MethodType.ServerStreaming:
                    {
                        var method = new Method<byte[], byte[]>(this.MethodType, this.ServiceName, this.MethodName, MagicOnionMarshallers.ThroughMarshaller, MagicOnionMarshallers.ThroughMarshaller);
                        var genericMethod = this.GetType()
                            .GetMethod(nameof(ServerStreamingServerMethod), BindingFlags.Instance | BindingFlags.NonPublic)!
                            .MakeGenericMethod(RequestType, UnwrappedResponseType);
                        var handler = (ServerStreamingServerMethod<byte[], byte[]>)genericMethod.CreateDelegate(typeof(ServerStreamingServerMethod<byte[], byte[]>), this);
                        binder.AddMethod(new MagicOnionServerMethod<byte[], byte[]>(method, this), handler);
                    }
                    break;
                case MethodType.DuplexStreaming:
                    {
                        BindDuplexStreamingHandler(binder, isStreamingHub);
                    }
                    break;
                default:
                    throw new InvalidOperationException("Unknown RegisterType:" + this.MethodType);
            }
        }

        async Task<TResponse?> UnaryServerMethod<TRequest, TResponse>(TRequest request, ServerCallContext context)
        {
            var isErrorOrInterrupted = false;
            var serviceContext = new ServiceContext(ServiceType, MethodInfo, AttributeLookup, this.MethodType, context, serializerOptions, logger, this, context.GetHttpContext().RequestServices);
            serviceContext.SetRawRequest(request);

            TResponse? response = default;
            try
            {
                logger.BeginInvokeMethod(serviceContext, typeof(TRequest));
                if (enableCurrentContext)
                {
                    ServiceContext.currentServiceContext.Value = serviceContext;
                }
                await this.methodBody(serviceContext).ConfigureAwait(false);
                if (serviceContext.Result is not null)
                {
                    response = (TResponse?)serviceContext.Result;
                }
            }
            catch (ReturnStatusException ex)
            {
                isErrorOrInterrupted = true;
                context.Status = ex.ToStatus();
                response = default;

                // WORKAROUND: Grpc.AspNetCore.Server throws a `Cancelled` status exception when it receives `null` response.
                //             To return the status code correctly, we needs to rethrow the exception here.
                //             https://github.com/grpc/grpc-dotnet/blob/d4ee8babcd90666fc0727163a06527ab9fd7366a/src/Grpc.AspNetCore.Server/Internal/CallHandlers/UnaryServerCallHandler.cs#L50-L56
                var rpcException = new RpcException(ex.ToStatus());
#if NET6_0_OR_GREATER
                if (ex.StackTrace is not null)
                {
                    ExceptionDispatchInfo.SetRemoteStackTrace(rpcException, ex.StackTrace);
                }
#endif
                throw rpcException;
            }
            catch (Exception ex)
            {
                isErrorOrInterrupted = true;
                if (isReturnExceptionStackTraceInErrorDetail)
                {
                    // Trim data.
                    var msg = ex.ToString();
                    var lineSplit = msg.Split(new[] { Environment.NewLine }, StringSplitOptions.None);
                    var sb = new System.Text.StringBuilder();
                    for (int i = 0; i < lineSplit.Length; i++)
                    {
                        if (!(lineSplit[i].Contains("System.Runtime.CompilerServices")
                           || lineSplit[i].Contains("直前に例外がスローされた場所からのスタック トレースの終わり")
                           || lineSplit[i].Contains("End of stack trace from the previous location where the exception was thrown")
                           ))
                        {
                            sb.AppendLine(lineSplit[i]);
                        }
                        if (sb.Length >= 5000)
                        {
                            sb.AppendLine("----Omit Message(message size is too long)----");
                            break;
                        }
                    }
                    var str = sb.ToString();

                    context.Status = new Status(StatusCode.Unknown, str);
                    logger.Error(ex, context);
                    response = default;
                }
                else
                {
                    throw;
                }
            }
            finally
            {
                logger.EndInvokeMethod(serviceContext, typeof(TResponse), (DateTime.UtcNow - serviceContext.Timestamp).TotalMilliseconds, isErrorOrInterrupted);
            }

            return response;
        }

        async Task<TResponse?> ClientStreamingServerMethod<TRequest, TResponse>(IAsyncStreamReader<TRequest> requestStream, ServerCallContext context)
        {
            var isErrorOrInterrupted = false;
            var serviceContext = new StreamingServiceContext<TRequest, Nil /* Dummy */>(
                ServiceType,
                MethodInfo,
                AttributeLookup,
                this.MethodType,
                context,
                serializerOptions,
                logger,
                this,
                context.GetHttpContext().RequestServices,
                requestStream,
                default
            );

            TResponse? response;
            try
            {
                using (requestStream as IDisposable)
                {
                    logger.BeginInvokeMethod(serviceContext, typeof(Nil));
                    if (enableCurrentContext)
                    {
                        ServiceContext.currentServiceContext.Value = serviceContext;
                    }
                    await this.methodBody(serviceContext).ConfigureAwait(false);
                    response = serviceContext.Result is TResponse r ? r : default;
                }
            }
            catch (ReturnStatusException ex)
            {
                isErrorOrInterrupted = true;
                context.Status = ex.ToStatus();
                response = default;
            }
            catch (Exception ex)
            {
                isErrorOrInterrupted = true;
                if (isReturnExceptionStackTraceInErrorDetail)
                {
                    context.Status = new Status(StatusCode.Unknown, ex.ToString());
                    logger.Error(ex, context);
                    response = default;
                }
                else
                {
                    throw;
                }
            }
            finally
            {
                logger.EndInvokeMethod(serviceContext, typeof(TResponse), (DateTime.UtcNow - serviceContext.Timestamp).TotalMilliseconds, isErrorOrInterrupted);
            }

            return response;
        }

        async Task<byte[]> ServerStreamingServerMethod<TRequest, TResponse>(TRequest request, IServerStreamWriter<TResponse> responseStream, ServerCallContext context)
        {
            var isErrorOrInterrupted = false;
            var serviceContext = new StreamingServiceContext<Nil /* Dummy */, TResponse>(
                ServiceType,
                MethodInfo,
                AttributeLookup,
                this.MethodType,
                context,
                serializerOptions, 
                logger,
                this,
                context.GetHttpContext().RequestServices,
                default,
                responseStream
            );
            serviceContext.SetRawRequest(request);
            try
            {
                logger.BeginInvokeMethod(serviceContext, typeof(TRequest));
                if (enableCurrentContext)
                {
                    ServiceContext.currentServiceContext.Value = serviceContext;
                }
                await this.methodBody(serviceContext).ConfigureAwait(false);
                return Array.Empty<byte>();
            }
            catch (ReturnStatusException ex)
            {
                isErrorOrInterrupted = true;
                context.Status = ex.ToStatus();
                return Array.Empty<byte>();
            }
            catch (Exception ex)
            {
                isErrorOrInterrupted = true;
                if (isReturnExceptionStackTraceInErrorDetail)
                {
                    context.Status = new Status(StatusCode.Unknown, ex.ToString());
                    logger.Error(ex, context);
                    return Array.Empty<byte>();
                }
                else
                {
                    throw;
                }
            }
            finally
            {
                logger.EndInvokeMethod(serviceContext, typeof(Nil), (DateTime.UtcNow - serviceContext.Timestamp).TotalMilliseconds, isErrorOrInterrupted);
            }
        }

        async Task<byte[]> DuplexStreamingServerMethod<TRequest, TResponse>(IAsyncStreamReader<TRequest> requestStream, IServerStreamWriter<TResponse> responseStream, ServerCallContext context)
        {
            var isErrorOrInterrupted = false;
            var serviceContext = new StreamingServiceContext<TRequest, TResponse>(
                ServiceType,
                MethodInfo,
                AttributeLookup,
                this.MethodType,
                context,
                serializerOptions,
                logger,
                this,
                context.GetHttpContext().RequestServices,
                requestStream,
                responseStream
            );
            try
            {
                logger.BeginInvokeMethod(serviceContext, typeof(Nil));
                using (requestStream as IDisposable)
                {
                    if (enableCurrentContext)
                    {
                        ServiceContext.currentServiceContext.Value = serviceContext;
                    }
                    await this.methodBody(serviceContext).ConfigureAwait(false);

                    return Array.Empty<byte>();
                }
            }
            catch (ReturnStatusException ex)
            {
                isErrorOrInterrupted = true;
                context.Status = ex.ToStatus();
                return Array.Empty<byte>();
            }
            catch (Exception ex)
            {
                isErrorOrInterrupted = true;
                if (isReturnExceptionStackTraceInErrorDetail)
                {
                    context.Status = new Status(StatusCode.Unknown, ex.ToString());
                    logger.Error(ex, context);
                    return Array.Empty<byte>();
                }
                else
                {
                    throw;
                }
            }
            finally
            {
                logger.EndInvokeMethod(serviceContext, typeof(Nil), (DateTime.UtcNow - serviceContext.Timestamp).TotalMilliseconds, isErrorOrInterrupted);
            }
        }

        public override string ToString()
        {
            return ServiceName + "/" + MethodName;
        }

        public override int GetHashCode()
        {
            return ServiceName.GetHashCode() ^ MethodInfo.Name.GetHashCode() << 2;
        }

        public bool Equals(MethodHandler? other)
        {
            return other != null && ServiceName.Equals(other.ServiceName) && MethodInfo.Name.Equals(other.MethodInfo.Name);
        }

        public class UniqueEqualityComparer : IEqualityComparer<MethodHandler>
        {
            public bool Equals(MethodHandler? x, MethodHandler? y)
            {
                return (x == null && y == null) || (x != null && y != null && x.methodHandlerId.Equals(y.methodHandlerId));
            }

            public int GetHashCode(MethodHandler obj)
            {
                return obj.methodHandlerId.GetHashCode();
            }
        }
    }

    /// <summary>
    /// Options for MethodHandler construction.
    /// </summary>
    public class MethodHandlerOptions
    {
        public IList<MagicOnionServiceFilterDescriptor> GlobalFilters { get; }

        public bool IsReturnExceptionStackTraceInErrorDetail { get; }

        public bool EnableCurrentContext { get; }

        public MessagePackSerializerOptions SerializerOptions { get; }

        public MethodHandlerOptions(MagicOnionOptions options)
        {
            GlobalFilters = options.GlobalFilters;
            IsReturnExceptionStackTraceInErrorDetail = options.IsReturnExceptionStackTraceInErrorDetail;
            EnableCurrentContext = options.EnableCurrentContext;
            SerializerOptions = options.SerializerOptions;
        }
    }

    internal class MethodHandlerResultHelper
    {
        static readonly ValueTask CopmletedValueTask = new ValueTask();

        public static ValueTask NewEmptyValueTask<T>(T result)
        {
            // ignore result.
            return CopmletedValueTask;
        }

        public static async ValueTask TaskToEmptyValueTask<T>(Task<T> result)
        {
            // wait and ignore result.
            await result;
        }

        public static async ValueTask SetUnaryResult<T>(UnaryResult<T> result, ServiceContext context)
        {
            if (result.hasRawValue)
            {
                context.Result = (result.rawTaskValue != null) ? await result.rawTaskValue.ConfigureAwait(false) : result.rawValue;
            }
        }

        public static async ValueTask SetTaskUnaryResult<T>(Task<UnaryResult<T>> taskResult, ServiceContext context)
        {
            var result = await taskResult.ConfigureAwait(false);
            if (result.hasRawValue)
            {
                context.Result = (result.rawTaskValue != null) ? await result.rawTaskValue.ConfigureAwait(false) : result.rawValue;
            }
        }

        public static ValueTask SerializeClientStreamingResult<TRequest, TResponse>(ClientStreamingResult<TRequest, TResponse> result, ServiceContext context)
        {
            if (result.hasRawValue)
            {
                var bytes = MessagePackSerializer.Serialize<TResponse>(result.rawValue, context.SerializerOptions);
                context.Result = bytes;
            }

            return default(ValueTask);
        }

        public static async ValueTask SerializeTaskClientStreamingResult<TRequest, TResponse>(Task<ClientStreamingResult<TRequest, TResponse>> taskResult, ServiceContext context)
        {
            var result = await taskResult.ConfigureAwait(false);
            if (result.hasRawValue)
            {
                var bytes = MessagePackSerializer.Serialize<TResponse>(result.rawValue, context.SerializerOptions);
                context.Result = bytes;
            }
        }
    }

    internal class MagicOnionServerMethod<TRequest, TResponse> : Method<TRequest, TResponse>
    {
        public MethodHandler MagicOnionMethodHandler { get; }
        public MagicOnionServerMethod(Method<TRequest, TResponse> method, MethodHandler magicOnionMethodHandler)
            : base(method.Type, method.ServiceName, method.Name, method.RequestMarshaller, method.ResponseMarshaller)
        {
            MagicOnionMethodHandler = magicOnionMethodHandler;
        }
    }

}